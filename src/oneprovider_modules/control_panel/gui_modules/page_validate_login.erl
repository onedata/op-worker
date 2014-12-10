%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page handles user validation via OpenID.
%% @end
%% ===================================================================

-module(page_validate_login).
-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/dao/dao_spaces.hrl").
-include("oneprovider_modules/dao/dao_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("dao/include/common.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Login page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    DisableThisPage = case application:get_env(?APP_Name, developer_mode) of
                          {ok, true} -> false;
                          _ -> true
                      end,
    case gui_ctx:user_logged_in() orelse DisableThisPage of
        true -> gui_jq:redirect(<<"/">>);
        false ->
            LoginMessage = case plgrid_openid_utils:prepare_validation_parameters() of
                               {error, invalid_request} -> {error, invalid_request};
                               {EndpointURL, RequestBody} ->
                                   plgrid_openid_utils:validate_openid_login({EndpointURL, RequestBody})
                           end,

            case LoginMessage of
                {error, invalid_request} ->
                    page_error:redirect_with_error(?error_openid_invalid_request);

                {error, auth_invalid} ->
                    page_error:redirect_with_error(?error_openid_auth_invalid);

                {error, no_connection} ->
                    page_error:redirect_with_error(?error_openid_no_connection);

                ok ->
                    try
                        case plgrid_openid_utils:retrieve_user_info() of
                            {error, invalid_request} ->
                                page_error:redirect_with_error(?error_openid_login_error);
                            {ok, Proplist} ->
                                % If a user logs in directly via PLGrid Openid, then mock spaces synchronization
                                case application:get_env(?APP_Name, spaces_mocked) of
                                    {ok, true} ->
                                        % But dont do it twice
                                        ok;
                                    _ ->
                                        Nodes = gen_server:call({global, central_cluster_manager}, get_nodes),
                                        AllNodes = Nodes -- [node()],

                                        SpacesBinary = [<<"space1">>, <<"space2">>],
                                        user_logic:create_user("12435", ["fake_user"], "Fake User", [], [], []),
                                        user_logic:create_user("abcde", ["fake_user2"], "Fake User", [], [], []),
                                        user_logic:create_user("54321", ["mock_user"], "Mock User", [], [], []),
                                        user_logic:create_user("9z8x7", ["dummy_user"], "Dummy User", [], [], []),

                                        mock(AllNodes, cluster_manager_lib, get_provider_id, fun() ->
                                            <<"providerId">> end),
                                        mock(AllNodes, fs_interface, get_provider_name, fun() ->
                                            <<"Mocked Provider Name">> end),
                                        mock(AllNodes, gr_users, get_spaces, fun(_) ->
                                            {ok, #user_spaces{ids = SpacesBinary, default = lists:nth(1, SpacesBinary)}} end),
                                        mock(AllNodes, gr_adapter, get_space_info, fun(SpaceId, _) ->
                                            Users = case SpaceId of
                                                        <<"space1">> -> [<<"12435">>, <<"abcde">>, <<"54321">>];
                                                        <<"space2">> -> [<<"12435">>, <<"9z8x7">>]
                                                    end,
                                            {ok, #space_info{space_id = SpaceId, name = SpaceId, providers = [<<"providerId">>], users = Users}} end)
                                end,

                                {Login, UserDoc} = user_logic:sign_in(Proplist, <<"">>, <<"">>, <<"">>),
                                LogoutToken = opn_gui_utils:gen_logout_token(),
                                gui_ctx:create_session(),
                                gui_ctx:set_user_id(UserDoc#db_document.uuid),
                                opn_gui_utils:set_global_user_id(UserDoc#db_document.record#user.global_id),
                                opn_gui_utils:set_user_fullname(user_logic:get_name(UserDoc)),
                                opn_gui_utils:set_user_role(user_logic:get_role(UserDoc)),
                                opn_gui_utils:set_logout_token(LogoutToken),
                                gui_jq:redirect_from_login(),
                                ?debug("User ~p logged in", [Login])
                        end
                    catch
                        throw:dir_creation_error ->
                            ?error_stacktrace("Error in validate_login - ~p:~p", [throw, dir_creation_error]),
                            page_error:redirect_with_error(?error_login_dir_creation_error);
                        throw:dir_chown_error ->
                            ?error_stacktrace("Error in validate_login - ~p:~p", [throw, dir_chown_error]),
                            page_error:redirect_with_error(?error_login_dir_chown_error);
                        Type:Message ->
                            ?error_stacktrace("Error in validate_login - ~p:~p", [Type, Message]),
                            page_error:redirect_with_error(?error_internal_server_error)
                    end
            end,
            <<"">>
    end.

event(init) -> ok;
event(terminate) -> ok.


% Used in development to mock spaces synchronization
mock(NodesUp, Module, Method, Fun) ->
    meck:new(Module, [passthrough, non_strict, unstick, no_link]),
    meck:expect(Module, Method, Fun),
    application:set_env(?APP_Name, spaces_mocked, true),
    {_, []} = rpc:multicall(NodesUp, meck, new, [Module, [passthrough, non_strict, unstick, no_link]]),
    {_, []} = rpc:multicall(NodesUp, meck, expect, [Module, Method, Fun]),
    {_, []} = rpc:multicall(application, set_env, [?APP_Name, spaces_mocked, true]).