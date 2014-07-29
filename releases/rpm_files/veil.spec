# Turn off binary file stripping
%global __os_install_post %{nil}

%define __prelink_undo_cmd %{nil}

%define _topdir     /tmp/veil_rpmbuild
%define _tmppath    %{_topdir}/tmp
%define _prefix     /opt/veil

%define name        veil
%define summary     Veil service
%define version     0.0.6
%define release     1
%define license     MIT
%define arch        x86_64
%define group       System/Base
%define source      %{name}.tar.gz
%define url         http://veil.com
%define vendor      veil
%define packager    veil
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
Veil service - allows installation of veil cluster nodes.

%prep

%setup -n %{name}

%build

%install
./install_rpm $RPM_BUILD_ROOT %{_prefix}

%post
mkdir -p /opt/veil/nodes
cp -r /opt/veil/files/onepanel_node /opt/veil/nodes/onepanel
sed -i s/"-name .*"/"-name onepanel@"`hostname -f`/g `find /opt/veil/nodes/onepanel/releases -name vm.args`
chkconfig --add veil
chkconfig --add onepanel
service onepanel start
ln -sf %{_prefix}/setup /usr/bin/veil_setup
ln -sf %{_prefix}/addusers /usr/bin/veil_addusers
ln -sf %{_prefix}/onepanel_setup /usr/bin/onepanel_setup

%preun
service veil stop
service onepanel stop
chkconfig --del veil
chkconfig --del onepanel
rm -f /usr/bin/veil_setup
rm -f /usr/bin/veil_addusers
rm -f /usr/bin/onepanel_setup
rm -rf %{_prefix}

%clean
rm -rf $RPM_BUILD_ROOT
rm -rf %{_tmppath}/%{name}
rm -rf %{_topdir}/BUILD/%{name}

# list files owned by the package here
%files
%defattr(-,root,root)
%{_prefix}
/etc/init.d/veil
/etc/init.d/onepanel

%changelog
* Sun Aug 02 2013 Veil
- 1.0 r1 First release
