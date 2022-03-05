# Abstract

***This independent research is supervised under Professor Wei WANG, HKUST. More details  regarding this project can be found in our research report under [Serverless Cold Start Research Report](https://github.com/chengyih001/knix_impl/blob/main/Serverless%20Cold%20Start%20Research%20Report.pdf)***

Serverless computing, or Function-as-a-Service (FAAS), provides users the ability to deploy applications and execution functions without paying additional effort in resource allocation and management. Cold start occurs when no warm instances are available when a function request arrives, thus a new execution instance must be spawned. Solutions to pre-warm instances are limited by memory capacity and therefore cannot scale up well. Swapping is a technique utilized in operating systems to free up additional physical memory by moving rarely used data to external storages. In this project, we apply swapping of pre-warm instances between memory and solid state drives to scale up the storing capacity and avoid cold start. Knix (https://github.com/knix-microfunctions/knix), a serverless platform designed by Bell Lab, is used as the framework for our project, as we revise and test the idea of swapping on it. 

\
\
***All following sections are identical to the original Knix Github.***

# Overview

![](logo/KNIX_logo_horizontal.jpg)

**KNIX MicroFunctions** is an open source serverless computing platform for Knative as well as bare metal or virtual machine-based environments.
It combines container-based resource isolation with a light-weight process-based execution model that significantly improves resource efficiency and decreases the function startup overhead.


Compared with existing serverless computing platforms, **KNIX MicroFunctions** has the following advantages:

* Low function startup latency & high resource efficiency
* Fast locality-aware storage access
* Python and Java function runtimes
* Workflow support and compatibility with [Amazon States Language (ASL)](https://states-language.net/spec.html)
* Support for long-running functions for continuous data processing applications
* Powerful web UI, SDK and CLI for effective serverless application development

# Screenshots

![](GUI/app/pages/docs/intro/mfn.gif?raw=true)

![](GUI/app/pages/docs/intro/wf_exec.gif?raw=true)


# Installation

This section covers installing KNIX.

### Installing KNIX MicroFunctions on Kubernetes

KNIX can be installed using helm charts, assuming you have a Kubernetes cluster and Knative running.

Please refer to the Helm package deployment [README](deploy/helm/microfunctions/README.md).

### Installing KNIX MicroFunctions on Bare Metal or Virtual Machines

KNIX MicroFunctions can also be installed using Ansible playbooks on bare metal or virtual machines. You'll need a user with sudo access.

Please refer to the installation [README](deploy/ansible/README.md).

# Hosted Service

More info on hosted services for hands-on experimentation with the **KNIX MicroFunctions** platform can be found at: [https://knix.io](https://knix.io).

# Getting Involved

We encourage you to participate in this open source project. We welcome pull requests, bug reports, ideas, code reviews, or any kind of positive contribution.

Before you attempt to make a contribution please read the [Code of Conduct](./CODE_OF_CONDUCT.md).

* [View current Issues](https://github.com/knix-microfunctions/knix/issues) or [view current Pull Requests](https://github.com/knix-microfunctions/knix/pulls).

* Join our Slack workspace [https://knix.slack.com](https://join.slack.com/t/knix/shared_invite/zt-dm7agzna-8~cVsYqAMKenxFhFDARjvw) which has 'users' and 'dev' channels.

* (Optionally) subscribe to [knix-users mailing list](https://groups.google.com/forum/#!forum/knix-users) and/or [knix-dev mailing list](https://groups.google.com/forum/#!forum/knix-dev).

# License

[Apache License 2.0](https://github.com/knix-microfunctions/knix/blob/master/LICENSE)

