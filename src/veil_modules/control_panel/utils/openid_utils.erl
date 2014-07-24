%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This library is used to authenticate users that have been redirected
%% from global registry.
%% @end
%% ===================================================================
-module(openid_utils).

-include_lib("ctool/include/logging.hrl").
-include_lib("veil_modules/control_panel/common.hrl").
-include_lib("veil_modules/dao/dao_users.hrl").

-define(user_login_prefix, "onedata_user_").

%% ====================================================================
%% API functions
%% ====================================================================
-export([validate_login/0]).

%% validate_login/0
%% ====================================================================
%% @doc
%% Authenticates a user via Global Registry.
%% Should be called from n2o page rendering context.
%% Upon error, returns predefined error id, which can be user to redirect
%% the user to error page.
%% @end
-spec validate_login() -> ok | {error, PredefinedErrorID :: atom()}.
%% ====================================================================
validate_login() ->
    try
        AuthorizationCode = gui_ctx:url_param(<<"code">>),

        KeyFile = application:get_env(veil_cluster_node, global_registry_provider_key_file, undefined),
        CertFile = application:get_env(veil_cluster_node, global_registry_provider_ca_cert_file, undefined),
        Opts = case {KeyFile, CertFile} of
                   {KeyF, CertF} when KeyF =:= undefined orelse CertF =:= undefined ->
                       error(no_certs_in_env);
                   {KeyF, CertF} ->
                       [{ssl_options, [{keyfile, atom_to_list(KeyF)}, {certfile, atom_to_list(CertF)}]}]
               end,

        Body = case ibrowse:send_req(?global_registry_hostname ++ ":8443/openid/token", [{"content-type", "application/json"}], post,
            "{\"code\":\"" ++ binary_to_list(AuthorizationCode) ++ "\", \"grant_type\":\"authorization_code\"}", Opts) of
                   {ok, "200", _, RespBody} ->
                       RespBody;
                   _ ->
                       throw(token_invalid)
               end,

        {struct, JSONProplist} = mochijson2:decode(Body),
        IDToken = proplists:get_value(<<"id_token">>, JSONProplist),
        [_Header, Payload, _Signature] = binary:split(IDToken, <<".">>, [global]),
        {struct, Proplist} = mochijson2:decode(base64decode(Payload)),

        GlobalID = gui_str:binary_to_unicode_list(proplists:get_value(<<"sub">>, Proplist)),
        Name = gui_str:binary_to_unicode_list(proplists:get_value(<<"name">>, Proplist)),
        EmailsBin = proplists:get_value(<<"email">>, Proplist),
        Emails = lists:map(fun({struct, [{<<"email">>, Email}]}) -> binary_to_list(Email) end, EmailsBin),
        Login = get_user_login(GlobalID),
        Teams = [],
        LoginProplist = [
            {global_id, GlobalID},
            {login, Login},
            {name, Name},
            {teams, Teams},
            {emails, Emails},
            {dn_list, []}
        ],
        try
            {Login, UserDoc} = user_logic:sign_in(LoginProplist),
            gui_ctx:create_session(),
            gui_ctx:set_user_id(Login),
            vcn_gui_utils:set_user_fullname(user_logic:get_name(UserDoc)),
            vcn_gui_utils:set_user_role(user_logic:get_role(UserDoc)),
            ?debug("User ~p logged in", [Login]),
            ok
        catch
            throw:dir_creation_error ->
                gui_ctx:clear_session(),
                ?error_stacktrace("Error in validate_login - ~p:~p", [throw, dir_creation_error]),
                {error, ?error_login_dir_creation_error};
            throw:dir_chown_error ->
                gui_ctx:clear_session(),
                ?error_stacktrace("Error in validate_login - ~p:~p", [throw, dir_chown_error]),
                {error, ?error_login_dir_chown_error};
            T:M ->
                gui_ctx:clear_session(),
                ?error_stacktrace("Error in validate_login - ~p:~p", [T, M]),
                {error, ?error_internal_server_error}
        end
    catch
        Type:Message ->
            ?error_stacktrace("Cannot validate login ~p:~p", [Type, Message]),
            {error, ?error_authentication}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% base64decode/0
%% ====================================================================
%% @doc
%% Decodes a base64 encoded term.
%% @end
-spec base64decode(binary() | string()) -> term().
%% ====================================================================
base64decode(Bin) when is_binary(Bin) ->
    Bin2 = case byte_size(Bin) rem 4 of
               2 -> <<Bin/binary, "==">>;
               3 -> <<Bin/binary, "=">>;
               _ -> Bin
           end,
    base64:decode(<<<<(urldecode_digit(D))>> || <<D>> <= Bin2>>);
base64decode(L) when is_list(L) ->
    base64decode(iolist_to_binary(L)).


%% urldecode_digit/0
%% ====================================================================
%% @doc
%% Urlencodes a single char in base64.
%% @end
-spec urldecode_digit(binary()) -> binary().
%% ====================================================================
urldecode_digit($_) -> $/;
urldecode_digit($-) -> $+;
urldecode_digit(D) -> D.


%% get_user_login/1
%% ====================================================================
%% @doc
%% Returns user login if it exists or generates a new one.
%% @end
-spec get_user_login(GlobalID :: string()) -> string().
%% ====================================================================
get_user_login(GlobalID) ->
    case user_logic:get_user({global_id, GlobalID}) of
        {ok, #veil_document{record = #user{login = Login}}} -> Login;
        _ -> next_free_user_login(1)
    end.


%% next_free_user_login/0
%% ====================================================================
%% @doc
%% Returns a user login string that is not occupied.
%% @end
-spec next_free_user_login(GlobalID :: string()) -> string().
%% ====================================================================
next_free_user_login(Counter) ->
    NewLogin = ?user_login_prefix ++ integer_to_list(Counter),
    case user_logic:get_user({login, NewLogin}) of
        {ok, _} -> next_free_user_login(Counter + 1);
        _ -> NewLogin
    end.



