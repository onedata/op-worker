%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

%% Wrapper for all models' records
-record(document, {
    key     :: datastore:key(),
    rev     :: term(),
    value   :: datastore:value(),
    links   :: term()
}).

-record(model_config, {
    name :: atom(),
    size = 0 :: non_neg_integer(),
    fields = [],
    defaults = {},
    hooks = [],
    bucket :: datastore:bucket()
}).
-define(MODEL_CONFIG(Bucket, Hooks), #model_config{name = ?MODULE,
                                                size = record_info(size, ?MODULE),
                                                fields = record_info(fields, ?MODULE),
                                                defaults = #?MODULE{},
                                                bucket = Bucket,
                                                hooks = Hooks}).

%% List of all available models
-define(MODELS, [some_record]).

%% Models' definitions

%% some_record - example model
-record(some_record, {field1, field2, field3}).