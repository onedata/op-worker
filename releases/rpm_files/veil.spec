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
requires: /bin/bash /bin/sh /usr/bin/env ld-linux-x86-64.so.2()(64bit) ld-linux-x86-64.so.2(GLIBC_2.3)(64bit)

%description
Veil service - allows installation of veil cluster nodes.

%prep

%setup -n %{name}

%build

%install
./install_rpm $RPM_BUILD_ROOT %{_prefix}

%post
chkconfig --add veil
ln -s %{_prefix}/setup /usr/bin/veil_setup

%preun
%{_prefix}/scripts/erl_launcher escript %{_prefix}/scripts/init.escript stop
chkconfig --del veil
rm -f /usr/bin/veil_setup

%clean
rm -rf $RPM_BUILD_ROOT
rm -rf %{_tmppath}/%{name}
rm -rf %{_topdir}/BUILD/%{name}

# list files owned by the package here
%files
%defattr(-,root,root)
%{_prefix}
/etc/init.d/veil

%changelog
* Sun Aug 02 2013 Veil
- 1.0 r1 First release
