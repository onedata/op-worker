%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains query string elements used for communication with OpenID Provider
%% @end
%% ===================================================================

-ifndef(OPENID_UTILS_HRL).
-define(OPENID_UTILS_HRL, 1).

%% Macros used for checkid_setup phase
-define(xrds_url, "https://openid.plgrid.pl/gateway").
-define(openid_checkid_mode, "openid.mode=checkid_setup").
-define(openid_ns, "openid.ns=http://specs.openid.net/auth/2.0").
-define(openid_return_to_prefix, "openid.return_to=https://"). % Hostname goes here
-define(openid_return_to_suffix, "/validate_login"). % Params go here
-define(openid_claimed_id, "openid.claimed_id=http://specs.openid.net/auth/2.0/identifier_select").
-define(openid_identity, "openid.identity=http://specs.openid.net/auth/2.0/identifier_select").
-define(openid_realm_prefix, "openid.realm=https://"). % Hostname goes here
-define(openid_sreg_required, "openid.sreg.required=nickname,email,fullname").
-define(openid_ns_ext1, "openid.ns.ext1=http://openid.net/srv/ax/1.0").
-define(openid_ext1_mode, "openid.ext1.mode=fetch_request").
-define(openid_ext1_type_dn1, "openid.ext1.type.dn1=http://openid.plgrid.pl/certificate/dn1").
-define(openid_ext1_type_dn2, "openid.ext1.type.dn2=http://openid.plgrid.pl/certificate/dn2").
-define(openid_ext1_type_dn3, "openid.ext1.type.dn3=http://openid.plgrid.pl/certificate/dn3").
-define(openid_ext1_type_teams, "openid.ext1.type.teams=http://openid.plgrid.pl/userTeams").
-define(openid_ext1_if_available, "openid.ext1.if_available=dn1,dn2,dn3,teams").


%% Macros used for check_authentication phase
-define(openid_check_authentication_mode, "openid.mode=check_authentication").
-define(openid_op_endpoint_key, "openid.op_endpoint").
-define(openid_signed_key, "openid.signed").
-define(openid_sig_key, "openid.sig").
-define(valid_auth_info, "is_valid:true\n").


% Macros used to extract parameters from OpenID response
% They are atoms so use of nitrogen wf:q is straightforward
-define(openid_login_key, 'openid.sreg.nickname').
-define(openid_name_key, 'openid.sreg.fullname').
-define(openid_teams_key, 'openid.ext1.value.teams').
-define(openid_email_key, 'openid.sreg.email').
-define(openid_dn1_key, 'openid.ext1.value.dn1').
-define(openid_dn2_key, 'openid.ext1.value.dn2').
-define(openid_dn3_key, 'openid.ext1.value.dn3').

-endif.

