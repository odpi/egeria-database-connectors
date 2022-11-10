<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

# Abstract Database caching event mapper Connector

This connector is an abstract event mapper that polls a 3rd party technology and puts that information into a en Egeria 
repository as part of the repository proxy pattern. 

This connector cannot be used by itself, it should be subclassed and the abstract methods implemented to access a 
particular technology. In this way the logic in this connector can be reused for different technologies.    

### Reference materials 

* [https://github.com/odpi/egeria/blob/main/open-metadata-implementation/repository-services/README.md](https://github.com/odpi/egeria/blob/main/open-metadata-implementation/repository-services/README.md)
  and it's sub-pages are great resources for developers.
* [Egeria Webinars](https://wiki.lfaidata.foundation/display/EG/Egeria+Webinar+program) particularly the one on repository connectors.


----

License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.

