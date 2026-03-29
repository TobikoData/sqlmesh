<p align="center">
  <img src="docs/readme/sqlmesh.png" alt="SQLMesh logo" width="50%" height="50%">
</p>
<p align="center">SQLMesh 是 <a href="https://www.linuxfoundation.org/">Linux 基金会</a>的一个项目</p>

SQLMesh 是一款下一代数据转换框架，旨在快速、高效且无错误地传输数据。数据团队可以运行和部署用SQL或Python编写的数据转换，且可在任何规模下实现可视化和控制。

它不仅仅是[DBT的替代方案](https://tobikodata.com/reduce_costs_with_cron_and_partitions.html)。

<p align="center">
  <img src="docs/readme/architecture_diagram.png" alt="Architecture Diagram" width="100%" height="100%">
</p>

## 核心功能

<img src="https://github.com/SQLMesh/sqlmesh-public-assets/blob/main/vscode.gif?raw=true" alt="SQLMesh Plan Mode">

> 在CLI和[SQLMesh VSCode扩展](https://sqlmesh.readthedocs.io/en/latest/guides/vscode/?h=vs+cod)中，都能即时获得SQL的影响和更改的上下文

  <details>
  <summary><b>虚拟数据环境</b></summary>

  * 查看[虚拟数据环境](https://whimsical.com/virtual-data-environments-MCT8ngSxFHict4wiL48ymz)工作的完整示意图
  * [观看此视频以了解更多](https://www.youtube.com/watch?v=weJH3eM0rzc)

  </details>

  * 创建无数据仓库成本的独立开发环境
  * 规划/应用像[Terraform](https://www.terraform.io/)这样的工作流程，以了解变更可能带来的影响
  * 易于使用的[CI/CD bot](https://sqlmesh.readthedocs.io/en/stable/integrations/github/)，适合真正的蓝绿部署

<details>
<summary><b>效率与测试</b></summary>

执行此命令将会生成一个单元测试文件 `tests/` : `test_stg_payments.yaml`

运行实时查询以生成模型的预期输出

```bash
sqlmesh create_test tcloud_demo.stg_payments --query tcloud_demo.seed_raw_payments "select * from tcloud_demo.seed_raw_payments limit 5"

# 运行单元测试
sqlmesh test
```

```sql
MODEL (
  name tcloud_demo.stg_payments,
  cron '@daily',
  grain payment_id,
  audits (UNIQUE_VALUES(columns = (
      payment_id
  )), NOT_NULL(columns = (
      payment_id
  )))
);

SELECT
    id AS payment_id,
    order_id,
    payment_method,
    amount / 100 AS amount, /* `amount 字段当前以美分为单位存储，所有我们将其转换为美元 */
    'new_column' AS new_column, /* 非破坏性变更的实例  */
FROM tcloud_demo.seed_raw_payments
```

```yaml
test_stg_payments:
model: tcloud_demo.stg_payments
inputs:
    tcloud_demo.seed_raw_payments:
      - id: 66
        order_id: 58
        payment_method: coupon
        amount: 1800
      - id: 27
        order_id: 24
        payment_method: coupon
        amount: 2600
      - id: 30
        order_id: 25
        payment_method: coupon
        amount: 1600
      - id: 109
        order_id: 95
        payment_method: coupon
        amount: 2400
      - id: 3
        order_id: 3
        payment_method: coupon
        amount: 100
outputs:
    query:
      - payment_id: 66
        order_id: 58
        payment_method: coupon
        amount: 18.0
        new_column: new_column
      - payment_id: 27
        order_id: 24
        payment_method: coupon
        amount: 26.0
        new_column: new_column
      - payment_id: 30
        order_id: 25
        payment_method: coupon
        amount: 16.0
        new_column: new_column
      - payment_id: 109
        order_id: 95
        payment_method: coupon
        amount: 24.0
        new_column: new_column
      - payment_id: 3
        order_id: 3
        payment_method: coupon
        amount: 1.0
        new_column: new_column
```
</details>

* 永远不要[重复](https://tobikodata.com/simplicity-or-efficiency-how-dbt-makes-you-choose.html)建立一个表
* 跟踪被修改的数据，并只运行[增量模型](https://tobikodata.com/correctly-loading-incremental-data-at-scale.html)所需的转换
* 免费运行[单元测试](https://tobikodata.com/we-need-even-greater-expectations.html)并配置自动审计
* 根据受变更影响的表/视图，运行生产和开发之间的[表差异](https://sqlmesh.readthedocs.io/en/stable/examples/sqlmesh_cli_crash_course/?h=crash#run-data-diff-against-prod)

<details>
<summary><b>提升你的SQL水平</b></summary>
用任何语言写 SQL，SQLMesh 都会在发送到仓库前实时转译成目标 SQL 语言。
<img src="https://github.com/SQLMesh/sqlmesh/blob/main/docs/readme/transpile_example.png?raw=true" alt="Transpile Example">
</details>

* 在仓库运行转换错误 *之前* ，先调试 [10+种不同的SQL语言](https://sqlmesh.readthedocs.io/en/stable/integrations/overview/#execution-engines)
* 仅使用 [simply SQL](https://sqlmesh.readthedocs.io/en/stable/concepts/models/sql_models/#sql-based-definition) 定义 (无需冗余和难懂的 `Jinja` + `YAML`)
* 在数据仓库中执行变更操作前，通过列级血缘关系查看变更的影响范围

对于更多信息, 请查看 [技术文档](https://sqlmesh.readthedocs.io/en/stable/).

## 入门指南
通过 [pypi](https://pypi.org/project/sqlmesh/) 安装 SQLMesh，
执行:

```bash
mkdir sqlmesh-example
cd sqlmesh-example
python -m venv .venv
source .venv/bin/activate
pip install 'sqlmesh[lsp]' # 安装带VSCode扩展功能的sqlmesh包
source .venv/bin/activate # 重新激活虚拟环境，确保使用的是正确的安装版本
sqlmesh init # 按照提示开始操作（选择DuckDB）
```

</details>

> 注意：根据你的 Python 安装情况，可能需要运行 `python3` 或 `pip3` 来代替 `python` or `pip`

<details>
<summary><b>Windows 安装</b></summary>

```bash
mkdir sqlmesh-example
cd sqlmesh-example
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install 'sqlmesh[lsp]' # 安装带VSCode扩展功能的sqlmesh包
.\.venv\Scripts\Activate.ps1 # 重新激活虚拟环境，确保使用的是正确的安装版本
sqlmesh init # 按照提示开始操作（选择DuckDB）
```
</details>


跟着 [快速入门指南](https://sqlmesh.readthedocs.io/en/stable/quickstart/cli/) 学习如何使用SQLMesh，你已经有了一个好的开始！

跟着 [速成课程](https://sqlmesh.readthedocs.io/en/stable/examples/sqlmesh_cli_crash_course/) 学习核心招式，并使用易于查阅的速查表。

跟着这个 [例子](https://sqlmesh.readthedocs.io/en/stable/examples/incremental_time_full_walkthrough/) 学习如何完整操作SQLMesh。

## 加入我们的社区
通过以下方式与我们联系：

* 加入 [Tobiko Slack社区](https://tobikodata.com/slack) ，提问或打个招呼吧！
* 请在我们的 [GitHub](https://github.com/SQLMesh/sqlmesh/issues/new) 提交问题
* 请通过 [hello@tobikodata.com](mailto:hello@tobikodata.com) 发送电子邮件，提出您的问题或反馈
* 阅读我们的 [blog](https://tobikodata.com/blog)

## 贡献
我们欢迎大家的贡献！请参阅 [CONTRIBUTING.md](CONTRIBUTING.md) 有关如何参与的指导方针，包括我们的DCO签字要求。

请查阅我们的 [行为准则](CODE_OF_CONDUCT.md) 和 [管理文件](GOVERNANCE.md) .

[阅读更多](https://sqlmesh.readthedocs.io/en/stable/development/) 关于如何搭建你的开发环境的信息。

## 许可说明
本项目采用 [Apache License 2.0](LICENSE) 授权。文档采用 [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/) 授权.
