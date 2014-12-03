%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides mapping of gui paths to modules that will
%% render the pages.
%% @end
%% ===================================================================

-module(gui_routes).
-include_lib("n2o/include/wf.hrl").
-include("oneprovider_modules/control_panel/global_registry_interfacing.hrl").
-export([init/2, finish/2]).

finish(State, Ctx) -> {ok, State, Ctx}.
init(State, Ctx) ->
    Path = wf:path(Ctx#context.req),
    RequestedPage = case Path of
                        <<"/ws", Rest/binary>> -> Rest;
                        Other -> Other
                    end,
    {ok, State, Ctx#context{path = Path, module = route(RequestedPage)}}.
route(<<"/">>) -> page_file_manager;
route(<<"/login">>) -> page_login;
route(<<"/logout">>) -> page_logout;
route(<<"/file_manager">>) -> page_file_manager;
route(<<"/shared_files">>) -> page_shared_files;
route(<<"/spaces">>) -> page_spaces;
route(<<"/space">>) -> page_space;
route(<<"/privileges/space">>) -> page_space_privileges;
route(<<"/groups">>) -> page_groups;
route(<<"/groups_old">>) -> page_groups_old;
route(<<"/group">>) -> page_group;
route(<<"/privileges/group">>) -> page_group_privileges;
route(<<"/tokens">>) -> page_tokens;
route(<<"/manage_account">>) -> page_manage_account;
route(<<"/about">>) -> page_about;
route(<<"/client_download">>) -> page_client_download;
route(<<"/privacy_policy">>) -> page_privacy_policy;
route(<<"/validate_login">>) -> page_validate_login;
route(<<"/openid/login">>) -> page_openid_login;
route(<<"/error">>) -> page_error;
route(ConnectionCheck) when ConnectionCheck == <<"/", (?connection_check_path)/binary>> -> page_connection_check;
route(_) -> page_404.