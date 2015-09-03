%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc
%% This page performs authentication of users that are redirected
%% from the Global Registry.
%% @end
%% ===================================================================
-module(page_validate_login).
-compile(export_all).

-include("modules/http_worker/http_common.hrl").
-include_lib("ctool/include/logging.hrl").

% @todo
% Create a new session based on macaroons and
% just print the information used in the process of logging in.
% This will be integrated with the new GUI when it is implemented.
main() ->
    SrlzdMacaroon = gui_ctx:url_param(<<"code">>),
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
    gui_str:format_bin(
        "Macaroon: ~s~n~n"
        "Discharge macaroons: ~s~n"
        "SessionId: ~s~n~n"
        "UserId: ~s~n",
        [Trim(SrlzdMacaroon), DMacsString, SessionId, UserId]).


event(init) -> ok;
event(terminate) -> ok.