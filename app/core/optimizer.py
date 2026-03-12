"""
参数优化器 — 遗传算法 + 网格搜索

支持:
  - ParameterSpace: 定义整数/浮点/离散参数空间
  - GeneticOptimizer: 锦标赛选择 + 均匀交叉 + 变异 + 精英保留 + 早停
  - GridSearch: 小参数空间的穷举搜索
  - OptimizationResult: 统一结果格式
"""

from __future__ import annotations

import itertools
import random
from dataclasses import dataclass, field
from typing import Any, Callable

from loguru import logger


# ═══════════════════════════════════════════
#  参数空间
# ═══════════════════════════════════════════


class ParameterSpace:
    """
    参数搜索空间定义。

    用法:
        space = ParameterSpace()
        space.add_int("fast_period", 5, 30)
        space.add_float("stop_loss_pct", 0.01, 0.10, step=0.01)
        space.add_choice("direction", ["long", "short", "both"])
    """

    def __init__(self):
        self._params: list[dict] = []

    def add_int(self, name: str, low: int, high: int, step: int = 1) -> "ParameterSpace":
        self._params.append({
            "name": name, "type": "int",
            "low": low, "high": high, "step": step,
        })
        return self

    def add_float(self, name: str, low: float, high: float,
                  step: float = None) -> "ParameterSpace":
        self._params.append({
            "name": name, "type": "float",
            "low": low, "high": high, "step": step,
        })
        return self

    def add_choice(self, name: str, choices: list) -> "ParameterSpace":
        self._params.append({
            "name": name, "type": "choice", "choices": choices,
        })
        return self

    def sample_random(self) -> dict:
        """随机采样一组参数。"""
        import numpy as np
        params = {}
        for p in self._params:
            if p["type"] == "int":
                values = list(range(p["low"], p["high"] + 1, p["step"]))
                params[p["name"]] = random.choice(values)
            elif p["type"] == "float":
                if p["step"] is not None:
                    values = np.arange(p["low"], p["high"] + p["step"] / 2, p["step"]).tolist()
                    params[p["name"]] = random.choice(values)
                else:
                    params[p["name"]] = random.uniform(p["low"], p["high"])
            elif p["type"] == "choice":
                params[p["name"]] = random.choice(p["choices"])
        return params

    def get_grid(self) -> list[dict]:
        """生成所有参数组合（用于网格搜索）。"""
        import numpy as np
        all_values = []
        names = []
        for p in self._params:
            names.append(p["name"])
            if p["type"] == "int":
                all_values.append(list(range(p["low"], p["high"] + 1, p["step"])))
            elif p["type"] == "float":
                if p["step"] is not None:
                    all_values.append(
                        np.arange(p["low"], p["high"] + p["step"] / 2, p["step"]).tolist()
                    )
                else:
                    all_values.append([p["low"], (p["low"] + p["high"]) / 2, p["high"]])
            elif p["type"] == "choice":
                all_values.append(p["choices"])

        grid = []
        for combo in itertools.product(*all_values):
            grid.append(dict(zip(names, combo)))
        return grid

    @property
    def total_combinations(self) -> int:
        return len(self.get_grid())

    @property
    def param_names(self) -> list[str]:
        return [p["name"] for p in self._params]

    def mutate(self, params: dict, mutation_rate: float = 0.2) -> dict:
        """对参数组合进行变异。"""
        import numpy as np
        result = params.copy()
        for p in self._params:
            if random.random() > mutation_rate:
                continue
            if p["type"] == "int":
                values = list(range(p["low"], p["high"] + 1, p["step"]))
                result[p["name"]] = random.choice(values)
            elif p["type"] == "float":
                if p["step"] is not None:
                    values = np.arange(p["low"], p["high"] + p["step"] / 2, p["step"]).tolist()
                    result[p["name"]] = random.choice(values)
                else:
                    result[p["name"]] = random.uniform(p["low"], p["high"])
            elif p["type"] == "choice":
                result[p["name"]] = random.choice(p["choices"])
        return result

    def crossover(self, parent_a: dict, parent_b: dict) -> dict:
        """均匀交叉: 每个参数随机取自父代 A 或 B。"""
        child = {}
        for p in self._params:
            name = p["name"]
            child[name] = parent_a[name] if random.random() < 0.5 else parent_b[name]
        return child


# ═══════════════════════════════════════════
#  优化结果
# ═══════════════════════════════════════════


@dataclass
class OptimizationResult:
    best_params: dict
    best_fitness: float
    all_results: list[dict] = field(default_factory=list)
    generations: int = 0
    total_evaluations: int = 0

    def top_n(self, n: int = 10) -> list[dict]:
        sorted_results = sorted(self.all_results, key=lambda x: x["fitness"], reverse=True)
        return sorted_results[:n]

    def summary(self) -> str:
        lines = [
            "═══ 优化结果摘要 ═══",
            f"最优参数:     {self.best_params}",
            f"最优适应度:   {self.best_fitness:.6f}",
            f"总评估次数:   {self.total_evaluations}",
            f"迭代代数:     {self.generations}",
            "",
            "─── Top 5 参数组合 ───",
        ]
        for i, r in enumerate(self.top_n(5)):
            lines.append(f"  #{i + 1}: fitness={r['fitness']:.6f}  params={r['params']}")
        return "\n".join(lines)


# ═══════════════════════════════════════════
#  遗传算法优化器
# ═══════════════════════════════════════════


class GeneticOptimizer:
    """
    遗传算法参数优化器。

    特性: 锦标赛选择 + 均匀交叉 + 随机变异 + 精英保留 + 早停
    """

    def __init__(
        self,
        space: ParameterSpace,
        fitness_fn: Callable[[dict], float],
        population_size: int = 50,
        generations: int = 30,
        tournament_size: int = 3,
        crossover_rate: float = 0.8,
        mutation_rate: float = 0.2,
        elite_count: int = 2,
        early_stop_generations: int = 10,
        seed: int = None,
    ):
        self.space = space
        self.fitness_fn = fitness_fn
        self.population_size = population_size
        self.generations = generations
        self.tournament_size = tournament_size
        self.crossover_rate = crossover_rate
        self.mutation_rate = mutation_rate
        self.elite_count = elite_count
        self.early_stop_generations = early_stop_generations

        if seed is not None:
            random.seed(seed)

    def run(self) -> OptimizationResult:
        population = [self.space.sample_random() for _ in range(self.population_size)]

        all_results: list[dict] = []
        best_fitness = float("-inf")
        best_params: dict = {}
        no_improve_count = 0

        for gen in range(self.generations):
            fitnesses = []
            for params in population:
                try:
                    fitness = self.fitness_fn(params)
                except Exception as e:
                    logger.warning(f"评估失败: {params} -> {e}")
                    fitness = float("-inf")

                fitnesses.append(fitness)
                all_results.append({"params": params.copy(), "fitness": fitness})

            gen_best_idx = max(range(len(fitnesses)), key=lambda i: fitnesses[i])
            gen_best_fitness = fitnesses[gen_best_idx]

            if gen_best_fitness > best_fitness:
                best_fitness = gen_best_fitness
                best_params = population[gen_best_idx].copy()
                no_improve_count = 0
            else:
                no_improve_count += 1

            logger.info(
                f"[Gen {gen + 1}/{self.generations}] "
                f"最优: {gen_best_fitness:.6f}  "
                f"全局最优: {best_fitness:.6f}  "
                f"无改善: {no_improve_count}"
            )

            if no_improve_count >= self.early_stop_generations:
                logger.info(f"早停: 连续 {self.early_stop_generations} 代无改善")
                break

            ranked = sorted(
                zip(population, fitnesses), key=lambda x: x[1], reverse=True,
            )
            new_population = [r[0].copy() for r in ranked[:self.elite_count]]

            while len(new_population) < self.population_size:
                parent_a = self._tournament_select(population, fitnesses)
                parent_b = self._tournament_select(population, fitnesses)
                if random.random() < self.crossover_rate:
                    child = self.space.crossover(parent_a, parent_b)
                else:
                    child = parent_a.copy()
                child = self.space.mutate(child, self.mutation_rate)
                new_population.append(child)

            population = new_population

        return OptimizationResult(
            best_params=best_params,
            best_fitness=best_fitness,
            all_results=all_results,
            generations=gen + 1,
            total_evaluations=len(all_results),
        )

    def _tournament_select(self, population: list[dict],
                           fitnesses: list[float]) -> dict:
        indices = random.sample(range(len(population)), min(self.tournament_size, len(population)))
        best_idx = max(indices, key=lambda i: fitnesses[i])
        return population[best_idx].copy()


# ═══════════════════════════════════════════
#  网格搜索
# ═══════════════════════════════════════════


class GridSearch:
    """穷举网格搜索。适用于参数空间 ≤1000 种组合。"""

    def __init__(
        self,
        space: ParameterSpace,
        fitness_fn: Callable[[dict], float],
    ):
        self.space = space
        self.fitness_fn = fitness_fn

    def run(self) -> OptimizationResult:
        grid = self.space.get_grid()
        total = len(grid)
        logger.info(f"网格搜索: 共 {total} 种参数组合")

        if total > 5000:
            logger.warning(f"参数组合数 {total} 较大，建议使用 GeneticOptimizer 替代")

        all_results: list[dict] = []
        best_fitness = float("-inf")
        best_params: dict = {}

        for i, params in enumerate(grid):
            try:
                fitness = self.fitness_fn(params)
            except Exception as e:
                logger.warning(f"评估失败: {params} -> {e}")
                fitness = float("-inf")

            all_results.append({"params": params.copy(), "fitness": fitness})
            if fitness > best_fitness:
                best_fitness = fitness
                best_params = params.copy()

            if (i + 1) % max(1, total // 10) == 0:
                logger.info(f"[{i + 1}/{total}] 当前最优: {best_fitness:.6f}")

        return OptimizationResult(
            best_params=best_params,
            best_fitness=best_fitness,
            all_results=all_results,
            generations=1,
            total_evaluations=total,
        )
