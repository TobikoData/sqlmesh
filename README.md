# About SQLMesh
SQLMesh is a next-generation SQL transformation platform. It provides you with powerful automation for versioning, backfilling, deployment, and testing -- allowing you to focus on simply writing SQL.

SQLMesh is able to achieve all of this with minimal setup; there are no additional services or dependencies required to get started using SQLMesh other than a connection to your existing data warehouse or engine.

## Why SQLMesh?

One of the main advantages over other transformation frameworks is that SQLMesh does not categorize incrementality as an "advanced" use case that should be avoided unless absolutely necessary. While other  frameworks default to full refresh compute, the default for SQLMesh is to optimize for incremental compute, i.e. computing one day or hour at a time. This allows SQLMesh to be faster and more scalable than other frameworks, allowing you to take advantage of the cost and time savings of incrementality.

SQLMesh also automates away complexity, so configuring models is no longer tricky due to complex macros that require understanding of the context for execution. Writing your data pipelines incrementally with SQLMesh not only saves you money and time, but keeps your systems maintainable, reliable, and accessible to all of your data practicioners.

## Reduced cost
As discussed above, incremental compute is significantly cheaper than full refresh compute.

For example, if you have one year of history but only receive new data on a daily basis, only processing that new data is ~365x cheaper than reprocessing one year each day. As your data grows, it's possible that refreshing your tables may take longer than a day, which means you would never be able to catch up!

In addition, you may not be able to refresh particular tables all at once; they may need to be batched into smaller intervals. The cost of your data pipelines compound as more dependent pipelines are created. Therefore, writing your data pipelines incrementally as much as possible can result in exponential savings.

## Increased efficiency
SQLMesh safely reuses physical tables across isolated environments. Some databases, such as Snowflake, have [zero-copy cloning](https://docs.snowflake.com/en/user-guide/tables-storage-considerations.html#label-cloning-tables) -- but this is a manual process, and not widely supported.

SQLMesh is able to automatically reuse tables regardless of which data warehouse or engine you're using. This is achieved by storing fingerprints of your models and by employing [views](https://en.wikipedia.org/wiki/View_(SQL)) like pointers to physical locations. Therefore, spinning up a new development environment is fast and cheap; only models with incompatible changes need to be materialized, once again saving time and money.

## Automation for everyone
Creating maintainable and scalable data pipelines is extremely difficult, and is a task usually reserved for data engineers. As your data grows, the need for incremental compute becomes mandatory due to the cost and time constaints.

Incremental models have inherent state of which partitions have been computed. This makes managing the consistency and accuracy challenging (leaving no data leakages or gaps). Although a seasoned engineer may have the expertise or tooling to operate one of these tables, an analyst would not. In these organizations, analysts would either need to file a ticket and wait on data engineering resources, or bypass core data models by running their own custom jobs, which inevitably leads to an ungoverned data mess. SQLMesh democratizes the ability to write safe and scalable data pipelines to all data practitioners, regardless of technical ability.

## Complexity made simple
As more and more models and users depend on core tables, the complexity of making changes increases. You must ensure that all downstream data consumers are compatible and updated with any new changes.

Propagating a change throughout a complex graph of dependencies is difficult to communicate, and also challenging to do accurately. The introduction of other schedulers such as [Airflow](https://airflow.apache.org/) adds even more complexity. SQLMesh seamlessly integrates directly with your existing scheduler so that your entire data pipeline, including jobs outside of SQLMesh, will be unified and robust.

## Collaboration and integration
SQLMesh allows for data pipelines to be a collaborative experience. It both empowers less technical data users to contribute and enables them to collaborate with others who may be more familiar with data engineering. Development can be done in a fully isolated environment that can be accessed and validated by others.

SQLMesh provides information about changes and how they may affect your downstream consumers. This transparency, along with the ability to categorize changes, makes it more feasible for a less technically savvy user to make updates to core data pipelines. By integrating with our Continuous Integration/Continuous Delivery (CI/CD) flows, you can require approval for any changes before going to production, ensuring that the relevant data owners or experts can review and validate the changes.

## Testing and reliability
SQLMesh supports both [audits](#audits) and [tests](#tests). Although unit tests has been commonplace in the world of software engineering, they are relatively unknown in the data world. SQLMesh's data unit tests allow for stability and reliability, as data pipeline owners can ensure that changes to models don't change underlying logic. These tests can run quickly in CI, or locally without having to create full scale tables.

Ready to jump in? Refer to `sqlmesh.docs.getting_started`.

# Community

We'd love to help guide you along your data journey. Follow the links below to connect with us:

* Join the [tobiko Slack community](https://join.slack.com/t/tobiko-data/shared_invite/zt-1je7o3xhd-C7~GuZTj0a8xz_uQbTJjHg) to ask questions, or just to say hi!
* File an issue on our [GitHub](https://github.com/TobikoData/sqlmesh/issues/new).
* Send us an email at [hello@tobikodata.com](hello@tobikodata.com) with your questions or feedback.
