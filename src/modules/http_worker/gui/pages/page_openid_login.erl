%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% This page performs authentication of users that are redirected
%% from the Global Registry.
%% @end
%% ===================================================================
-module(page_openid_login).
-compile(export_all).
-include("modules/http_worker/http_common.hrl").
-include_lib("ctool/include/logging.hrl").

main() ->
    case openid_utils:validate_login() of
        {error, ErrorID} ->
            gui_str:format("Error: ~p~n", [ErrorID]);
        Props ->
            gui_str:format_bin("~p", [Props])
    end.


event(init) -> ok;
event(terminate) -> ok.