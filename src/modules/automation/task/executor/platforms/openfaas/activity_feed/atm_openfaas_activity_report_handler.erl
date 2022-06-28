%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for a module that handles OpenFaaS activity reports.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_activity_report_handler).
-author("Lukasz Opiola").


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback consume_report(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_activity_report:body(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    {no_reply | {reply_json, json_utils:json_term()}, atm_openfaas_activity_feed_ws_handler:handler_state()}.


-callback handle_error(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    errors:error(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    ok.
