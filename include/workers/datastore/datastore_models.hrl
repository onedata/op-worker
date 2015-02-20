%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Models definitions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_MODELS_HRL).
-define(DATASTORE_MODELS_HRL, 1).

%% Definition of session model
-include("models/session.hrl").

%% Wrapper for all models' records
-record(document, {
    key :: datastore:key(),
    rev :: term(),
    value :: datastore:value(),
    links :: term()
}).

%% Models' definitions

%% sample model with example fields
-record(some_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% sequencer dispatcher model:
%% node - node on which sequencer dispatcher has been started
%% pid  - pid of sequencer dispatcher associated with client session
%% sup  - pid of sequencer dispatcher supervisor
-record(sequencer_dispatcher_data, {
    node :: node(),
    pid :: pid(),
    sup :: pid()
}).

%% event dispatcher model:
%% node - node on which event dispatcher has been started
%% pid  - pid of event dispatcher associated with client session
%% sup  - pid of event dispatcher supervisor
-record(event_dispatcher_data, {
    node :: node(),
    pid :: pid(),
    sup :: pid()
}).

%% event manager model:
%% value - mapping from subscription ID to subscription
-record(subscription, {
    value :: event_manager:subscription()
}).

-endif.