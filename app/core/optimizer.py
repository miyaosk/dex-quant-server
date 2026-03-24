"""
参数优化器 — 五种搜索算法

支持:
  - ParameterSpace: 定义整数/浮点/离散参数空间
  - GridSearch: 小参数空间的穷举搜索
  - RandomSearch: 高维随机采样
  - GeneticOptimizer: 锦标赛选择 + 均匀交叉 + 变异 + 精英保留 + 早停
  - BayesianOptimizer: TPE 代理模型，少量评估快速收敛
  - SimulatedAnnealing: 模拟退火，跳出局部最优
  - ParticleSwarmOptimizer: 粒子群，群体协作搜索
  - OptimizationResult: 统一结果格式
"""

from __future__ import annotations

import itertools
import math
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


# ═══════════════════════════════════════════
#  随机搜索
# ═══════════════════════════════════════════


class RandomSearch:
    """随机采样搜索。高维空间比网格高效，实现简单。"""

    def __init__(
        self,
        space: ParameterSpace,
        fitness_fn: Callable[[dict], float],
        n_samples: int = 200,
    ):
        self.space = space
        self.fitness_fn = fitness_fn
        self.n_samples = n_samples

    def run(self) -> OptimizationResult:
        logger.info(f"随机搜索: 采样 {self.n_samples} 组")

        all_results: list[dict] = []
        best_fitness = float("-inf")
        best_params: dict = {}

        for i in range(self.n_samples):
            params = self.space.sample_random()
            try:
                fitness = self.fitness_fn(params)
            except Exception as e:
                logger.warning(f"评估失败: {params} -> {e}")
                fitness = float("-inf")

            all_results.append({"params": params.copy(), "fitness": fitness})
            if fitness > best_fitness:
                best_fitness = fitness
                best_params = params.copy()

            if (i + 1) % max(1, self.n_samples // 10) == 0:
                logger.info(f"[{i + 1}/{self.n_samples}] 当前最优: {best_fitness:.6f}")

        return OptimizationResult(
            best_params=best_params,
            best_fitness=best_fitness,
            all_results=all_results,
            generations=1,
            total_evaluations=self.n_samples,
        )


# ═══════════════════════════════════════════
#  贝叶斯优化 (Tree-structured Parzen Estimator)
# ═══════════════════════════════════════════


class BayesianOptimizer:
    """
    TPE 贝叶斯优化器。

    将已评估样本分为 good / bad 两组（按 gamma 分位），
    对每个参数分别建核密度估计，采样时最大化 l(x)/g(x)。
    评估次数少时也能快速收敛。
    """

    def __init__(
        self,
        space: ParameterSpace,
        fitness_fn: Callable[[dict], float],
        n_initial: int = 20,
        n_iterations: int = 80,
        gamma: float = 0.25,
        n_candidates: int = 64,
    ):
        self.space = space
        self.fitness_fn = fitness_fn
        self.n_initial = n_initial
        self.n_iterations = n_iterations
        self.gamma = gamma
        self.n_candidates = n_candidates

    def run(self) -> OptimizationResult:
        total = self.n_initial + self.n_iterations
        logger.info(f"贝叶斯优化(TPE): 初始 {self.n_initial} + 迭代 {self.n_iterations} = {total} 次评估")

        all_results: list[dict] = []
        best_fitness = float("-inf")
        best_params: dict = {}

        for i in range(self.n_initial):
            params = self.space.sample_random()
            fitness = self._safe_eval(params)
            all_results.append({"params": params.copy(), "fitness": fitness})
            if fitness > best_fitness:
                best_fitness = fitness
                best_params = params.copy()

        logger.info(f"初始采样完成, 当前最优: {best_fitness:.6f}")

        for i in range(self.n_iterations):
            params = self._tpe_sample(all_results)
            fitness = self._safe_eval(params)
            all_results.append({"params": params.copy(), "fitness": fitness})
            if fitness > best_fitness:
                best_fitness = fitness
                best_params = params.copy()

            step = i + self.n_initial + 1
            if step % max(1, total // 10) == 0:
                logger.info(f"[{step}/{total}] 当前最优: {best_fitness:.6f}")

        return OptimizationResult(
            best_params=best_params,
            best_fitness=best_fitness,
            all_results=all_results,
            generations=1,
            total_evaluations=len(all_results),
        )

    def _safe_eval(self, params: dict) -> float:
        try:
            return self.fitness_fn(params)
        except Exception as e:
            logger.warning(f"评估失败: {params} -> {e}")
            return float("-inf")

    def _tpe_sample(self, results: list[dict]) -> dict:
        """TPE 采样：分 good/bad 组，对每个参数建核密度，选 l(x)/g(x) 最大的候选。"""
        sorted_r = sorted(results, key=lambda x: x["fitness"], reverse=True)
        n_good = max(1, int(len(sorted_r) * self.gamma))
        good = [r["params"] for r in sorted_r[:n_good]]
        bad = [r["params"] for r in sorted_r[n_good:]] or [r["params"] for r in sorted_r]

        best_score = float("-inf")
        best_candidate = None

        for _ in range(self.n_candidates):
            candidate = self.space.sample_random()
            score_good = self._kde_score(candidate, good)
            score_bad = self._kde_score(candidate, bad)
            ratio = score_good - score_bad
            if ratio > best_score:
                best_score = ratio
                best_candidate = candidate

        return best_candidate or self.space.sample_random()

    @staticmethod
    def _kde_score(candidate: dict, samples: list[dict]) -> float:
        """简化的核密度估计（对数得分）。"""
        if not samples:
            return 0.0
        log_density = 0.0
        for key in candidate:
            val = candidate[key]
            col_vals = [s[key] for s in samples]
            if isinstance(val, (int, float)):
                nums = [float(v) for v in col_vals if isinstance(v, (int, float))]
                if not nums:
                    continue
                mean = sum(nums) / len(nums)
                var = max(sum((x - mean) ** 2 for x in nums) / len(nums), 1e-8)
                log_density += -0.5 * ((float(val) - mean) ** 2) / var
            else:
                matches = sum(1 for v in col_vals if v == val)
                log_density += math.log(max(matches / len(col_vals), 0.01))
        return log_density


# ═══════════════════════════════════════════
#  模拟退火
# ═══════════════════════════════════════════


class SimulatedAnnealing:
    """
    模拟退火优化器。

    高温时接受差解跳出局部最优，逐步降温收敛。
    """

    def __init__(
        self,
        space: ParameterSpace,
        fitness_fn: Callable[[dict], float],
        n_iterations: int = 200,
        temp_start: float = 1.0,
        temp_end: float = 0.01,
        cooling_rate: float = None,
    ):
        self.space = space
        self.fitness_fn = fitness_fn
        self.n_iterations = n_iterations
        self.temp_start = temp_start
        self.temp_end = temp_end
        self.cooling_rate = cooling_rate or (temp_end / temp_start) ** (1.0 / max(n_iterations, 1))

    def run(self) -> OptimizationResult:
        logger.info(f"模拟退火: {self.n_iterations} 次迭代, T: {self.temp_start} → {self.temp_end}")

        current = self.space.sample_random()
        current_fitness = self._safe_eval(current)
        best_params = current.copy()
        best_fitness = current_fitness

        all_results: list[dict] = [{"params": current.copy(), "fitness": current_fitness}]
        temp = self.temp_start

        for i in range(self.n_iterations):
            neighbor = self.space.mutate(current, mutation_rate=0.4)
            neighbor_fitness = self._safe_eval(neighbor)
            all_results.append({"params": neighbor.copy(), "fitness": neighbor_fitness})

            delta = neighbor_fitness - current_fitness
            if delta > 0 or (temp > 0 and random.random() < math.exp(delta / max(temp, 1e-10))):
                current = neighbor
                current_fitness = neighbor_fitness

            if current_fitness > best_fitness:
                best_fitness = current_fitness
                best_params = current.copy()

            temp *= self.cooling_rate

            if (i + 1) % max(1, self.n_iterations // 10) == 0:
                logger.info(f"[{i + 1}/{self.n_iterations}] T={temp:.4f} 最优: {best_fitness:.6f}")

        return OptimizationResult(
            best_params=best_params,
            best_fitness=best_fitness,
            all_results=all_results,
            generations=1,
            total_evaluations=len(all_results),
        )

    def _safe_eval(self, params: dict) -> float:
        try:
            return self.fitness_fn(params)
        except Exception as e:
            logger.warning(f"评估失败: {params} -> {e}")
            return float("-inf")


# ═══════════════════════════════════════════
#  粒子群优化 (PSO)
# ═══════════════════════════════════════════


class ParticleSwarmOptimizer:
    """
    粒子群优化器。

    每个粒子记住个体最优位置，同时向全局最优靠拢。
    离散参数通过概率映射处理。
    """

    def __init__(
        self,
        space: ParameterSpace,
        fitness_fn: Callable[[dict], float],
        n_particles: int = 30,
        n_iterations: int = 50,
        w: float = 0.7,
        c1: float = 1.5,
        c2: float = 1.5,
    ):
        self.space = space
        self.fitness_fn = fitness_fn
        self.n_particles = n_particles
        self.n_iterations = n_iterations
        self.w = w
        self.c1 = c1
        self.c2 = c2

    def run(self) -> OptimizationResult:
        logger.info(f"粒子群优化: {self.n_particles} 粒子 × {self.n_iterations} 迭代")

        particles = [self.space.sample_random() for _ in range(self.n_particles)]
        velocities = [{} for _ in range(self.n_particles)]
        p_best = [p.copy() for p in particles]
        p_best_fitness = [float("-inf")] * self.n_particles
        g_best: dict = {}
        g_best_fitness = float("-inf")
        all_results: list[dict] = []

        for it in range(self.n_iterations):
            for i, params in enumerate(particles):
                fitness = self._safe_eval(params)
                all_results.append({"params": params.copy(), "fitness": fitness})

                if fitness > p_best_fitness[i]:
                    p_best_fitness[i] = fitness
                    p_best[i] = params.copy()

                if fitness > g_best_fitness:
                    g_best_fitness = fitness
                    g_best = params.copy()

            for i in range(self.n_particles):
                particles[i] = self._update_particle(
                    particles[i], velocities[i], p_best[i], g_best,
                )

            if (it + 1) % max(1, self.n_iterations // 10) == 0:
                logger.info(f"[{it + 1}/{self.n_iterations}] 全局最优: {g_best_fitness:.6f}")

        return OptimizationResult(
            best_params=g_best,
            best_fitness=g_best_fitness,
            all_results=all_results,
            generations=self.n_iterations,
            total_evaluations=len(all_results),
        )

    def _update_particle(self, pos: dict, vel: dict, p_best: dict, g_best: dict) -> dict:
        """更新粒子位置，离散/选择参数用概率切换。"""
        new_pos = {}
        for p in self.space._params:
            name = p["name"]
            if p["type"] in ("int", "float"):
                v = vel.get(name, 0.0)
                v = (self.w * v
                     + self.c1 * random.random() * (self._to_num(p_best.get(name, 0)) - self._to_num(pos.get(name, 0)))
                     + self.c2 * random.random() * (self._to_num(g_best.get(name, 0)) - self._to_num(pos.get(name, 0))))
                vel[name] = v
                new_val = self._to_num(pos.get(name, 0)) + v
                new_val = max(p["low"], min(p["high"], new_val))
                if p["type"] == "int":
                    step = p.get("step", 1)
                    new_val = round((new_val - p["low"]) / step) * step + p["low"]
                    new_pos[name] = int(max(p["low"], min(p["high"], new_val)))
                else:
                    step = p.get("step")
                    if step:
                        new_val = round((new_val - p["low"]) / step) * step + p["low"]
                    new_pos[name] = round(max(p["low"], min(p["high"], new_val)), 8)
            elif p["type"] == "choice":
                prob_switch = self.c1 * random.random() * 0.1 + self.c2 * random.random() * 0.1
                if random.random() < prob_switch:
                    new_pos[name] = g_best.get(name, pos.get(name))
                else:
                    new_pos[name] = pos.get(name)
        return new_pos

    @staticmethod
    def _to_num(val) -> float:
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    def _safe_eval(self, params: dict) -> float:
        try:
            return self.fitness_fn(params)
        except Exception as e:
            logger.warning(f"评估失败: {params} -> {e}")
            return float("-inf")
