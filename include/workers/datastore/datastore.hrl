%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common definions and configurations for datastore.
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


%% This record shall not be used outside datastore engine and shall not be instantiate
%% directly. Use MODEL_CONFIG macro instead.
-record(model_config, {
    name :: model_behaviour:model_type(),
    size = 0 :: non_neg_integer(),
    fields = [],
    defaults = {},
    hooks = [] :: [{model_behaviour:model_type(), model_behaviour:model_action()}],
    bucket :: datastore:bucket()
}).

%% Helper macro for instantiating #model_config record.
%% Bucket - see #model_config.bucket
%% Hooks :: see #model_config.hooks
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