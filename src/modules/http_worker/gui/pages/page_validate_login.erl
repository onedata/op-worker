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
-module(page_validate_login).
-compile(export_all).
-include("modules/http_worker/http_common.hrl").
-include_lib("ctool/include/logging.hrl").

% For now, just print the information that came from GR.
main() ->
    SrlzdMacaroon = gui_ctx:url_param(<<"code">>),
    {ok, #auth{} = Auth} = auth_utils:authorize(SrlzdMacaroon),

%%     ?dump(SerializedMacaroon),
%%     {ok, Macaroon} = macaroon:deserialize(SerializedMacaroon),
%%     ?dump(Macaroon),
%%     {ok, InspectData4} = macaroon:inspect(Macaroon),
%%     io:format([InspectData4, "\n"]),
%%     {ok, Caveats} = macaroon:third_party_caveats(Macaroon),
%%     ?dump(Caveats),
%%     [{_, CaveatId}] = Caveats,
%%     application:set_env(ctool, verify_server_cert, false),
%%     ?dump(jiffy:encode({[{<<"identifier">>, CaveatId}]})),
%%     {ok, SDM} = gui_utils:https_post(<<"https://172.17.0.72:8443/user/authorize">>,
%%         [{<<"content-type">>, <<"application/json">>}],
%%         jiffy:encode({[{<<"identifier">>, CaveatId}]})),
%%     {ok, DM} = macaroon:deserialize(SDM),
%%     {ok, InspectData5} = macaroon:inspect(DM),
%%     {ok, Macaroon} = macaroon:deserialize(SrlzdMacaroon),
%%     {ok, DM} = macaroon:deserialize(hd(SrlzdDischMacaroons)),
%%     {ok, DoublePenetration} = macaroon:prepare_for_request(Macaroon, DM),
%%     {ok, SDP} = macaroon:serialize(DoublePenetration),
%%     Res = gr_endpoint:auth_request(
%%         provider,
%%         "/user/",
%%         get,
%%         [
%%             {"macaroon", binary_to_list(SrlzdMacaroon)},
%%             {"discharge-macaroons", binary_to_list(SDP)}
%%         ],
%%         "",
%%         []
%%     ),
    {ok, SessionId} = session_manager:create_gui_session(Auth),
    ?dump(SessionId),
    ?dump(session:get_auth(SessionId)),

    <<"hehe3">>.


event(init) -> ok;
event(terminate) -> ok.