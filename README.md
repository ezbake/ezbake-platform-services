# How to Build RPMs #
In order to build RPMs, run the following command where `<git clone URI>` is replaced by the URI used to clone this repo
and `<browser URL>` is replaced by the URL used to view the code in a browser.

```bash
mvn clean install -Prpmbuild -Dproject.scm.connection=scm:git:<git clone URI> -Dproject.scm.url=<browser URL>
```

Each RPM will be placed in the `target/rpm` directory for each of the projects with an `rpmbuild` Maven profile.