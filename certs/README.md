# Certificates directory

By default, this directory should contain WEB key/cert pair, 
under the following names:

* web_cert.pem
* web_key.pem

The default paths can be changed in *app.config* file.

## WEB certificates

### Production environment

You should use certificates signed by a trusted CA to ensure secure connections 
for clients. If your certificates were bundled with a CA chain, place it in the
*cacerts* directory (*../cacerts*) under any filename with *.pem* extension. 

### Test environment

For test purposes, if no web certificates are found, new ones will be 
automatically generated. They are signed by OnedataTestCA 
(*../cacerts/OneDataTestWebServerCa.pem*). By including this CA in your trusted
certificates you can test Onedata with "green" HTTPS connections.

**WARNING** Test certificates can be used only for testing purposes and by no 
means ensure safe connections. Both key and cert of OnedataTestCA are publicly
available.
