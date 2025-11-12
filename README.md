A out of process (aka disaggregated) optimizer for Presto

- Based on Cascades
- Uses Datafusion plan shapes (or Substrait plans decoded to Datafusion plans)
- Does NOT use the Datafusion planner
- Started life as a Rust port of a proof-of-concept [Java cascades implementation](https://github.ibm.com/Anant-Aneja/disagg_optimizer/tree/java_joinorder_optimizer)

### Tracking Project

https://github.ibm.com/orgs/lakehouse/projects/208/views/2