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

-include("veil_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").
-include_lib("veil_modules/control_panel/common.hrl").
-include_lib("veil_modules/dao/dao_users.hrl").

-define(user_login_prefix, "onedata_user_").

%% ====================================================================
%% API functions
%% ====================================================================
-export([validate_login/0, get_user_login/1, refresh_access/3]).

%% validate_login/0
%% ====================================================================
%% @doc
%% Authenticates a user via Global Registry.
%% Should be called from n2o page rendering context.
%% Upon error, returns predefined error id, which can be used to redirect
%% the user to error page.
%% @end
-spec validate_login() -> ok | {error, PredefinedErrorID :: atom()}.
%% ====================================================================
validate_login() ->
    AuthorizationCode = gui_ctx:url_param(<<"code">>),
    Request = [{<<"code">>, AuthorizationCode}, {<<"grant_type">>, <<"authorization_code">>}],

    case authorize_user(Request) of
        {error, Reason} -> {error, Reason};
        {RefreshToken, ExpiresIn} ->
            #context{session = SessionId} = ?CTX,
            schedule_refresh(RefreshToken, ExpiresIn, SessionId)
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% get_user_login/1
%% ====================================================================
%% @doc
%% Returns user login if it exists or generates a new one.
%% @end
-spec get_user_login(GRUID :: string()) -> string().
%% ====================================================================
get_user_login(GRUID) ->
    case user_logic:get_user({global_id, GRUID}) of
        {ok, #veil_document{record = #user{login = Login}}} -> Login;
        _ -> next_free_user_login(1)
    end.


%% next_free_user_login/0
%% ====================================================================
%% @doc
%% Returns a user login string that is not occupied.
%% @end
-spec next_free_user_login(GRUID :: string()) -> string().
%% ====================================================================
next_free_user_login(Counter) ->
    NewLogin = ?user_login_prefix ++ integer_to_list(Counter),
    case user_logic:get_user({login, NewLogin}) of
        {ok, _} -> next_free_user_login(Counter + 1);
        _ -> NewLogin
    end.


%% schedule_refresh/3
%% ====================================================================
%% @doc Schedules a refresh of user's access.
-spec schedule_refresh(RefreshToken :: binary(), ExpiresIn :: non_neg_integer(),
                       SessionId :: uuid()) -> ok.
%% ====================================================================
schedule_refresh(RefreshToken, ExpiresIn, SessionId) ->
    RefreshedAccessToken = vcn_gui_utils:get_access_token(),
    RefreshAfter = timer:seconds(trunc(ExpiresIn * 4 / 5)),
    timer:apply_after(RefreshAfter, ?MODULE, refresh_access,
                      [RefreshToken, RefreshedAccessToken, SessionId]),
    ok.


%% refresh_access/3
%% ====================================================================
%% @doc Refresh user's access and schedule a next refresh.
-spec refresh_access(RefreshToken :: binary(), RefreshedAccessToken :: binary(),
                     SessionId :: uuid()) -> ok.
%% ====================================================================
refresh_access(RefreshToken, RefreshedAccessToken, SessionId) ->
    Context = wf_context:init_context([]),
    wf_context:context(Context#context{session = SessionId}),

    case vcn_gui_utils:get_access_token() of
        RefreshedAccessToken ->
            Request = [{<<"refresh_token">>, RefreshToken}, {<<"grant_type">>, <<"refresh_token">>}],
            case authorize_user(Request) of
                {error, Reason} ->
                    ?error("Couldn't refresh user's access: ~p", [Reason]),
                    gui_ctx:clear_session();

                {NewRefreshToken, ExpiresIn} ->
                    schedule_refresh(NewRefreshToken, ExpiresIn, SessionId)
            end;

    %% the user has obtained a new access token since the refresh has been
    %% scheduled, or the session expired
        _ -> ok
    end.


%% authorize_user/1
%% ====================================================================
%% @doc Authorizes the user access and saves his credentials in all the right
%% places.
%% @end
-spec authorize_user(Request :: proplists:proplist()) ->
    {RefreshToken :: binary(), ExpiresIn :: non_neg_integer()} | {error, PredefinedErrorID :: atom()}.
%% ====================================================================
authorize_user(Request) ->
    try
        {ok, #token_response{
            access_token = AccessToken,
            expires_in = ExpiresIn,
            refresh_token = RefreshToken,
            id_token = #id_token{
                sub = GRUID,
                name = Name,
                emails = EmailList}
        }} = gr_openid:get_token_response(provider, Request),
        Login = get_user_login(gui_str:binary_to_unicode_list(GRUID)),
        LoginProplist = [
            {global_id, gui_str:binary_to_unicode_list(GRUID)},
            {login, Login},
            {name, gui_str:binary_to_unicode_list(Name)},
            {teams, []},
            {emails, lists:map(fun(Email) -> gui_str:binary_to_unicode_list(Email) end, EmailList)},
            {dn_list, []}
        ],
        try
            {Login, UserDoc} = user_logic:sign_in(LoginProplist, AccessToken),
            gui_ctx:create_session(),
            gui_ctx:set_user_id(Login),
            vcn_gui_utils:set_global_user_id(gui_str:binary_to_unicode_list(GRUID)),
            vcn_gui_utils:set_access_token(AccessToken),
            vcn_gui_utils:set_user_fullname(user_logic:get_name(UserDoc)),
            vcn_gui_utils:set_user_role(user_logic:get_role(UserDoc)),
            vcn_gui_utils:set_logout_token(vcn_gui_utils:gen_logout_token()),
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
        end,
        {RefreshToken, ExpiresIn}
    catch
        Type:Message ->
            ?error_stacktrace("Cannot validate login ~p:~p", [Type, Message]),
            {error, ?error_authentication}
    end.
