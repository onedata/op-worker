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

%% Wrapper for all models' records
-record(document, {
    key :: datastore:key(),
    rev :: term(),
    value :: datastore:value(),
    links :: term()
}).

%% Models' definitions

%% sample model with example fields
-record(sample_model, {field1, field2, field3}).

%% sequencer dispatcher model:
%% node - node on which sequencer dispatcher has been started
%% pid  - pid of sequencer dispatcher associated with client session
%% sup  - pid of sequencer dispatcher supervisor
-record(sequencer_dispatcher_model, {node, pid, sup}).

%% sequencer manager model:
%% node - node on which event dispatcher has been started
%% pid  - pid of event dispatcher associated with FUSE client
%% sup  - pid of event dispatcher supervisor
-record(event_dispatcher_model, {node, pid, sup}).

-endif.