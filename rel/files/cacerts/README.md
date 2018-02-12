# CA certs directory

This directory is used too add trusted certificates to the application.
By default, a standard CA bundle is used, so in most cases there
is no need to add additional certificates. If your environment uses its
own certification, your should add your trusted chain here.

To add certificates, just place files in this dir. Each file can contain any 
number of certificates. The application will read the whole dir, no matter the 
file names.

### Certs sharing with Onepanel

If the oz-worker application is started via Onepanel, this directory is NOT 
used, but oz-worker uses the same certificates as Onepanel.

#### NOTE

This directory is NOT used to add trusted chain to your web cert, in such
case please see the README in certs directory.