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

-ifndef(DATASTORE_HRL).
-define(DATASTORE_HRL, 1).

-include("modules/datastore/datastore_models.hrl").

%% This record shall not be used outside datastore engine and shall not be instantiated
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


%% Common predicates
-define(PRED_ALWAYS, fun() -> true end).


%% Utils
-define(RESPONSE(R), begin
                         {ok, Response} = R,
                         Response
                     end
).


%% Common funs
-define(GET_ALL,
    fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (Obj, Acc) ->
            {next, [Obj | Acc]}
    end).


%% List of all available models
-define(MODELS, [
    some_record,
    subscription,
    session,
    onedata_user,
    identity
]).

-endif.
