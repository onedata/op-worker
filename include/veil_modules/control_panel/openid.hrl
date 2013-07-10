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

-ifndef(OPENID_HRL).
-define(OPENID_HRL, 1).

-define(xrds_url, "https://openid.plgrid.pl/gateway").
-define(openid_ns, "openid.ns=http://specs.openid.net/auth/2.0").
-define(openid_return_to, "openid.return_to=https://veilfsdev.com:8000/validate_login").
-define(openid_claimed_id, "openid.claimed_id=http://specs.openid.net/auth/2.0/identifier_select").
-define(openid_identity, "openid.identity=http://specs.openid.net/auth/2.0/identifier_select").
-define(openid_realm, "openid.realm=https://veilfsdev.com:8000").
-define(openid_sreg_required, "openid.sreg.required=nickname,email,fullname").
-define(openid_login_response_key_list,
  ['openid.ns','openid.op_endpoint','openid.claimed_id','openid.response_nonce',
    'openid.mode','openid.identity','openid.return_to','openid.assoc_handle',
    'openid.signed','openid.sig','openid.ns.sreg','openid.sreg.nickname',
    'openid.sreg.email','openid.sreg.fullname']).

-define(invalid_auth_info, "is_valid:false\n").
-define(valid_auth_info, "is_valid:true\n").

-endif.