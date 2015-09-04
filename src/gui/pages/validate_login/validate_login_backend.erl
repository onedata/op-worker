%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 15:25
%%%-------------------------------------------------------------------
-module(validate_login_backend).
-author("lopiola").

-compile([export_all]).

-include("modules/http_worker/http_common.hrl").
-include_lib("ctool/include/logging.hrl").

page_init() ->
    SrlzdMacaroon = g_ctx:get_url_param(<<"code">>),
    {ok, Auth = #auth{
        disch_macaroons = DMacaroons}} = gui_auth_manager:authorize(SrlzdMacaroon),
    {ok, SessionId} = session_manager:create_gui_session(Auth),
    {ok, #document{
        value = #session{
            identity = #identity{user_id = UserId}}}} = session:get(SessionId),

    % Print retrieved info
    % Fun to cut too long strings
    Trim = fun(Binary) ->
        case byte_size(Binary) > 80 of
            true ->
                <<(binary:part(Binary, {0, 80}))/binary, "...">>;
            false ->
                Binary
        end
    end,

    DMacsString = lists:foldl(
        fun(DM, Acc) ->
            % Padding for pretty print
            gui_str:format_bin("~s~s~n                     ",
                [Acc, binary_to_list(Trim(DM))])
        end, "", DMacaroons),
    Body = gui_str:format_bin(
        "Macaroon: ~s~n~n"
        "Discharge macaroons: ~s~n"
        "SessionId: ~s~n~n"
        "UserId: ~s~n",
        [Trim(SrlzdMacaroon), DMacsString, SessionId, UserId]),
    {serve_body, Body}.