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
    key     :: datastore:key(),
    rev     :: term(),
    value   :: datastore:value(),
    links   :: term()
}).


%% Models' definitions

%% some_record - example model
-record(some_record, {field1, field2, field3}).
-record(sequencer_model, {}).

-endif.