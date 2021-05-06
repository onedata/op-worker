%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_args).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_task_execution.hrl").
-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([build_specs/2, build_args/2]).


%% Full 'input_spec' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"inputRefType">> := <<"const">> | <<"store">> | <<"item">>,
%%      <<"inputRef">> => case <<"inputRefType">> of
%%          <<"const">> -> ConstValue :: json_utils:json_term();
%%          <<"store">> -> StoreSchemaId :: binary();
%%          <<"item">> -> json_utils:query() % optional, if not specified entire
%%                                           % item is substituted
%%      end
%% }
-type input_spec() :: map().

-record(atm_task_execution_argument_spec, {
    name :: binary(),
    input_spec :: input_spec(),
    data_spec :: atm_data_spec:record(),
    is_array :: boolean()
}).
-type spec() :: #atm_task_execution_argument_spec{}.

-export_type([input_spec/0, spec/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_specs([atm_lambda_argument_spec()], [atm_task_schema_argument_mapper()]) ->
    [spec()].
build_specs(AtmLambdaArgSpecs, AtmTaskSchemaArgMappers) ->
    build_specs(
        lists:usort(fun order_atm_lambda_arg_specs_by_name/2, AtmLambdaArgSpecs),
        lists:usort(fun order_atm_task_schema_arg_mappers_by_name/2, AtmTaskSchemaArgMappers),
        []
    ).


-spec build_args([spec()], atm_task_execution:ctx()) -> json_utils:json_map().
build_args(AtmTaskExecutionArgSpecs, AtmTaskExecutionCtx) ->
    lists:foldl(fun(#atm_task_execution_argument_spec{
        name = Name,
        input_spec = InputSpec
    } = AtmTaskExecutionArgSpec, Args) ->
        ArgValue = build_arg(AtmTaskExecutionCtx, InputSpec),
        validate_arg(ArgValue, AtmTaskExecutionArgSpec),
        Args#{Name => ArgValue}
    end, #{}, AtmTaskExecutionArgSpecs).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec order_atm_lambda_arg_specs_by_name(
    atm_lambda_argument_spec(),
    atm_lambda_argument_spec()
) ->
    boolean().
order_atm_lambda_arg_specs_by_name(
    #atm_lambda_argument_spec{name = Name1},
    #atm_lambda_argument_spec{name = Name2}
) ->
    Name1 =< Name2.


%% @private
-spec order_atm_task_schema_arg_mappers_by_name(
    atm_task_schema_argument_mapper(),
    atm_task_schema_argument_mapper()
) ->
    boolean().
order_atm_task_schema_arg_mappers_by_name(
    #atm_task_schema_argument_mapper{name = Name1},
    #atm_task_schema_argument_mapper{name = Name2}
) ->
    Name1 =< Name2.


%% @private
-spec build_specs(
    OrderedUniqueAtmLambdaArgSpecs :: [atm_lambda_argument_spec()],
    OrderedUniqueAtmTaskSchemaArgMappers :: [atm_task_schema_argument_mapper()],
    AtmTaskExecutionArgSpecs :: [spec()]
) ->
    [spec()] | no_return().
build_specs([], [], AtmTaskExecutionArgSpecs) ->
    AtmTaskExecutionArgSpecs;

build_specs(
    [#atm_lambda_argument_spec{name = Name} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    [#atm_task_schema_argument_mapper{name = Name} = AtmTaskSchemaArgMapper | RestAtmTaskSchemaArgMappers],
    AtmTaskExecutionArgSpecs
) ->
    build_specs(RestAtmLambdaArgSpecs, RestAtmTaskSchemaArgMappers, [
        build_spec(AtmLambdaArgSpec, AtmTaskSchemaArgMapper) | AtmTaskExecutionArgSpecs
    ]);

build_specs(
    [#atm_lambda_argument_spec{default_value = Default} = AtmLambdaArgSpec | RestAtmLambdaArgSpecs],
    AtmTaskSchemaArgMappers,
    AtmTaskExecutionArgSpecs
) when Default /= undefined ->
    build_specs(RestAtmLambdaArgSpecs, AtmTaskSchemaArgMappers, [
        build_spec(AtmLambdaArgSpec, undefined), AtmTaskExecutionArgSpecs
    ]);

build_specs(
    [#atm_lambda_argument_spec{is_optional = true} | RestAtmLambdaArgSpecs],
    AtmTaskSchemaArgMappers,
    AtmTaskExecutionArgSpecs
) ->
    build_specs(RestAtmLambdaArgSpecs, AtmTaskSchemaArgMappers, AtmTaskExecutionArgSpecs);

build_specs(_AtmLambdaArgSpecs, _AtmTaskSchemaArgMappers, _AtmTaskExecutionArgSpecs) ->
    throw(?EINVAL).


%% @private
-spec build_spec(
    atm_lambda_argument_spec(),
    undefined | atm_task_schema_argument_mapper()
) ->
    spec().
build_spec(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_array = IsArray,
    default_value = DefaultValue
}, undefined) ->
    #atm_task_execution_argument_spec{
        name = Name,
        input_spec = #{
            <<"inputRefType">> => <<"const">>,
            <<"inputRef">> => DefaultValue
        },
        data_spec = AtmDataSpec,
        is_array = IsArray
    };

build_spec(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_array = IsArray
}, #atm_task_schema_argument_mapper{input_spec = InputSpec}) ->
    #atm_task_execution_argument_spec{
        name = Name,
        input_spec = InputSpec,
        data_spec = AtmDataSpec,
        is_array = IsArray
    }.


%% @private
-spec build_arg(atm_task_execution:ctx(), input_spec()) ->
    json_utils:json_term() | no_return().
build_arg(_AtmTaskExecutionArgSpec, #{
    <<"inputRefType">> := <<"const">>,
    <<"inputRef">> := ConstValue
}) ->
    ConstValue;

build_arg(#atm_task_execution_ctx{stores = Stores}, #{
    <<"inputRefType">> := <<"store">>,
    <<"inputRef">> := StoreSchemaId
}) ->
    case maps:get(StoreSchemaId, Stores, undefined) of
        undefined -> throw(?EINVAL);
        StoreCredentials -> StoreCredentials
    end;

build_arg(#atm_task_execution_ctx{item = Item}, #{
    <<"inputRefType">> := <<"item">>
} = InputSpec) ->
    case maps:get(<<"inputRef">>, InputSpec, undefined) of
        undefined ->
            Item;
        Path ->
            % TODO fix query in case of array indices
            case json_utils:query(Item, Path) of
                {ok, Value} -> Value;
                error -> throw(?EINVAL)
            end
    end;

build_arg(_AtmTaskExecutionArgSpec, _InputSpec) ->
    throw(?EINVAL).


%% @private
-spec validate_arg(json_utils:json_term() | [json_utils:json_term()], spec()) ->
    ok | no_return().
validate_arg(ArgsBatch, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_array = true
}) ->
    lists:foreach(fun(ArgValue) ->
        atm_data_validator:assert_instance(ArgValue, AtmDataSpec)
    end, ArgsBatch);

validate_arg(ArgValue, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_array = false
}) ->
    atm_data_validator:assert_instance(ArgValue, AtmDataSpec).
