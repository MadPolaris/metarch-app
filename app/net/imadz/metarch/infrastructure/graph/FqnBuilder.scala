package net.imadz.metarch.infrastructure.graph

/**
 * FqnBuilder——problem/solution 双 FQN 链式构造器（多空间身份模型 S-B）。
 *
 * 与算法内核（backend-codegen evtgraph.FqnBuilder）逐行镜像；契约同步靠两侧用例镜像维护
 * （v2 可抽共享 lib）。渲染契约 / kindTag 白名单详见内核版 scaladoc：
 * 根段裸写，其后 `tag:值` 以 '.' 连接；solution 段缺省则跳过，全空 ⇒ solutionPending=true
 * （设计书开放问题3：纯业务概念合法存在）。
 */
object FqnBuilder {

  /** kindTag 白名单（v1 = 设计文档出现过的九种；扩展 = KindTags 加名 + Builder 加便捷方法） */
  val KindTags: Vector[String] = Vector("sd", "ctx", "ag", "ev", "cmd", "fld", "inv", "pol", "uc")
  private val Whitelist: Set[String] = KindTags.toSet

  final case class Step(tag: String, problemSeg: String, solutionSeg: String)

  final case class FqnPair(problemFqn: String, solutionFqn: Option[String]) {
    def solutionPending: Boolean = solutionFqn.isEmpty
  }

  final case class Builder private[graph] (projectProblem: String, projectSolution: String,
                                           steps: Vector[Step] = Vector.empty) {

    private[graph] def add(tag: String, p: String, s: String): Builder = {
      require(Whitelist.contains(tag), s"kindTag '$tag' 不在白名单：${KindTags.mkString("/")}")
      require(p != null && p.nonEmpty, s"kindTag $tag 的 problem 段不能为空")
      copy(steps = steps :+ Step(tag, p, if (s != null && s.nonEmpty) s else null))
    }

    // 便捷方法（名称对齐设计书 §四 示例链）：第二参数缺省 = 该段不进 solution 链
    def sub(p: String, s: String = null): Builder = add("sd", p, s)
    def ctx(p: String, s: String = null): Builder = add("ctx", p, s)
    def agg(p: String, s: String = null): Builder = add("ag", p, s)
    def evt(p: String, s: String = null): Builder = add("ev", p, s)
    def cmd(p: String, s: String = null): Builder = add("cmd", p, s)
    def fld(p: String, s: String = null): Builder = add("fld", p, s)
    def inv(p: String, s: String = null): Builder = add("inv", p, s)
    def pol(p: String, s: String = null): Builder = add("pol", p, s)
    def uc(p: String, s: String = null): Builder = add("uc", p, s)

    /** 定稿渲染：双 FQN 各自独立成串 */
    def build(): FqnPair = {
      val sol = steps.collect { case Step(t, _, v) if v != null => s"$t:$v" }
      FqnPair(
        (projectProblem +: steps.map(s => s"${s.tag}:${s.problemSeg}")).mkString("."),
        if (sol.isEmpty) None else Some((projectSolution +: sol).mkString(".")))
    }
  }

  /** 双空间根段入口：problemRoot=业务域前缀（net.imadz.信贷…），solutionRoot=代码域前缀 */
  def project(problemRoot: String, solutionRoot: String): Builder =
    Builder(problemRoot, solutionRoot)

  /** 程序化批量入口：steps = (tag, problem, solution 可空) */
  def fromSteps(problemRoot: String, solutionRoot: String, steps: Seq[(String, String, String)]): FqnPair =
    steps.foldLeft(Builder(problemRoot, solutionRoot)) { (b, x) => b.add(x._1, x._2, x._3) }.build()
}
