# Turn off binary file stripping
%global __os_install_post %{nil}

%define __prelink_undo_cmd %{nil}
%define debug_package %{nil}

%define _topdir     /tmp/oneprovider_rpmbuild
%define _tmppath    %{_topdir}/tmp
%define _prefix     /opt/oneprovider

%define name        oneprovider
%define summary     oneprovider service
%define version     {{version}}
%define release     1
%define license     MIT
%define arch        x86_64
%define group       System/Base
%define source      %{name}.tar.gz
%define url         http://provider.example.com
%define vendor      onedata
%define packager    onedata
%define buildroot   %{_tmppath}/%{name}-build


Name:      %{name}
Version:   %{version}
Release:   %{release}
Packager:  %{packager}
Vendor:    %{vendor}
License:   %{license}
Summary:   %{summary}
Group:     %{group}
Source:    %{source}
URL:       %{url}
Prefix:    %{_prefix}
BuildRoot: %{buildroot}
BuildArch: %{arch}

# Disable auto dependency recognition and list required deps explicitely
AutoReqProv: no
requires: /bin/bash /bin/sh /usr/bin/env ld-linux-x86-64.so.2()(64bit) ld-linux-x86-64.so.2(GLIBC_2.3)(64bit) rrdtool

%description
oneprovider service - allows installation of oneprovider nodes.

%prep

%setup -n %{name}

%build

%install
./install_rpm $RPM_BUILD_ROOT %{_prefix}

%post
sh %{_prefix}/onepanel_setup %{_prefix}
chkconfig --add oneprovider
chkconfig --add onepanel
service onepanel start
ln -sf %{_prefix}/setup /usr/bin/oneprovider_setup
ln -sf %{_prefix}/addusers /usr/bin/oneprovider_addusers
ln -sf %{_prefix}/onepanel_admin /usr/bin/onepanel_admin

%preun
service oneprovider stop
service onepanel stop
chkconfig --del oneprovider
chkconfig --del onepanel
rm -f /usr/bin/oneprovider_setup
rm -f /usr/bin/oneprovider_addusers
rm -f /usr/bin/onepanel_admin
rm -rf %{_prefix}
rm -rf /opt/bigcouch

%clean
rm -rf $RPM_BUILD_ROOT
rm -rf %{_tmppath}/%{name}
rm -rf %{_topdir}/BUILD/%{name}

# list files owned by the package here
%files
%defattr(-,root,root)
%{_prefix}
/etc/init.d/oneprovider
/etc/init.d/onepanel

%changelog
* Sun Aug 02 2013 oneprovider
- 1.0 r1 First release
