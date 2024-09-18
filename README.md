# dbt-graph-detective

A command line tool for searching [dbt's manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json).

Have you ever tried using dbt's [graph operators](https://docs.getdbt.com/reference/node-selection/graph-operators)
to find something particular, but found it wasn't possible? 
Even with [yaml selectors](https://docs.getdbt.com/reference/node-selection/yaml-selectors) it may not be possible.

Often the shortcoming is that you cannot reference the _context_ of a node using graph selectors.
eg: you cannot search for all nodes which have no children.

## pre-commit hooks

This tool can be used to lint that your datamodel structure doesn't violate specific conditions.
It can be run via [pre-commit](https://pre-commit.com/) like so:

~~~ yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/kingalban/dbt-graph-detective
    rev: <choose a git revision: aabb321> 
    hooks:
      - id: final_dynamic_tables_have_lag
        args: ["--dbt-project-dir=./<path-to-your-dbt-project>"]
        stages: [manual] # Add this to only trigger manually (in case you don't always have the manifest ready)
~~~

### final_dynamic_tables_have_lag
dbt-snowflake's provides a [dynamic_table](https://docs.getdbt.com/reference/resource-configs/snowflake-configs#dynamic-tables) materialization type.
Snowflake will automatically update dynamic tables according to their target lag, or their downstream target lag.
Failing to define a literal target lag for the final table in a DAG of dynamic tables will result in the data **never** being updated.
Note that `dbt run` **does not** refresh the table.

eg: guards against this situation, where models `B` and `C` won't be refreshed (arrows point downstream)
~~~
[A target_lag: DOWNSTREAM] ─> [B target_lag: DOWNSTREAM] ─> [C target_lab: DOWNSTREAM]
                         └──> [D target_lag: 15 minutes]
~~~


### literal_tables_not_downstream_dynamic_tables
dbt-snowflake's dynamic tables are not updated when running `dbt run`, only on their own schedule, or with `--full-refresh`.
Therefore, data does not flow through the DAG as you might expect with `dbt run`. 
There will be a lag introduced by dynamic tables.

eg: guards against this situation, where models `C` and `D` won't show new data from `A` 
until **both** the dynamic table `B` has refreshed itself, and `dbt run` is run again.
~~~
[A table] ─> [B dynamic_table] ─> [C table]
                             └──> [D incremental]
~~~
