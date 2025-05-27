# Security Overview


At Tobiko, we treat security as a first class citizen because we know how valuable your data assets are. Our team follows and executes security best practices across each layer of our product. 

## Tobiko Cloud Standard Deployment

Our standard Tobiko Cloud deployment consists of several components that are each responsible for different parts of the product. 

Below is a diagram of the components along with their description. 

![tobiko_cloud_standard_deployment](./tcloud_standard_deployment.png){ width=80% height=60% style="display: block; margin: 0 auto" }

- **Scheduler**: Orchestrates schedule cadence and hosts state metadata(code versions, logs, cost)
- **Executor**: Applies code changes and runs SQL queries (actual data processing in SQL Engine) and python models in proper DAG order.
- **Gateway**: Stores authentication to SQL Engine. Secured through encryption.
- **SQL Engine**: Processes and stores data based on the above instructions within the **customer’s** environment.

## Tobiko Cloud Hybrid Deployment

For some customers, our hybrid deployment option is a great fit. It provides a seamless experience with Tobiko Cloud but in your own VPC and infrastructure.  

In a hybrid deployment, Tobiko Cloud does not execute tasks directly with the engine. Instead, it passes tasks to the executors hosted in your environment, which then execute the tasks with the engine. 

Executors are Docker containers that connect to both Tobiko Cloud and your SQL engine. They pull work tasks from the Tobiko Cloud scheduler and execute them with your SQL engine.

Below is a diagram of the components along with their description. 

![tobiko_cloud_hybrid_deployment](./tcloud_hybrid_deployment.png){ width=80% height=60% style="display: block; margin: 0 auto" }


- **Helm Chart**: For production environements, we provide a [Helm chart](./hybrid_executors_helm.md) that includes robust configurability, secret management, and scaling options.
- **Docker Compose**: For simpler environments or testing, we offer a [Docker Compose setup](./hybrid_executors_docker_compose) to quickly deploy executors on any machine with Docker.


## Internal Code Practices

Our coding standards are guidelines we enforce throughout the organization to write, maintain, and collaborate on code effecively. These practice ensure consistency, maintainability, realibility, and most importantly, trust. 

Below you will find a few examples of our interal code requirements. 

- We have signed commits, required approvers, and signed docker artifacts.
- Each commit to main is approved by someone different than the author.
- We follow the standard of signing commits and then registering the key with GitHub. [Github Docs](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits)
- Binary is signed using cosign and OIDC for keyless. [Signing docs](https://docs.sigstore.dev/cosign/signing/overview/)
 

## Physical Property 

### How do we protect PGP keys?

If an employee loses their laptop, we don't need to get the old PGP key back. We use GitHub to sign code. At the time the code was committed, the PGP key was valid. When an employee loses their laptop, we will invalidate it, and they will regenerate a new key. The old commits are valid because the PGP key was valid at the time.

### How do we invalidate PGP keys if someone did steal it and could potentially use it?

Revoke access for the GitHub user account associated with the compromised key and not give them access again until the old PGP key is deprecated and issue a new PGP key.

### If someone steals a laptop, what's our continuity plan in protecting code?

- All employee devices are monitored for appropriate encryption and password policies
- Laptop protection is done through file encryption for Vanta
- Mandatory lock screen after a timeout
- We have a procedure for the disposal of an IT asset to mitigate keys being compromised through inappropriate disposal
- See above for PGP key protection
