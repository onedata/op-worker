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
%% sequencer manager model:
%% node - node on which sequencer manager has been started
%% pid  - pid of sequencer manager associated with FUSE client
%% sup  - pid of sequencer manager supervisor
-record(sequencer_manager_model, {node, pid, sup}).

-endif.