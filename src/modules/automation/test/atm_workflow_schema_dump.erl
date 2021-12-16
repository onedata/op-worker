%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper function for management of automation workflow schema dump.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_schema_dump).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_workflow_schema_dump.hrl").

-export([create_from_draft/1]).
-export([to_json/1]).


-type atm_openfaas_operation_spec_draft() :: #atm_openfaas_operation_spec_draft{}.
-type atm_lambda_argument_spec_draft() :: #atm_lambda_argument_spec_draft{}.
-type atm_lambda_result_spec_draft() :: #atm_lambda_result_spec_draft{}.
-type atm_lambda_revision_draft() :: #atm_lambda_revision_draft{}.

-type atm_store_iterator_spec_draft() :: #atm_store_iterator_spec_draft{}.
-type atm_task_argument_value_builder_draft() :: #atm_task_argument_value_builder_draft{}.
-type atm_task_schema_argument_mapper_draft() :: #atm_task_schema_argument_mapper_draft{}.
-type atm_task_schema_result_mapper_draft() :: #atm_task_schema_result_mapper_draft{}.
-type atm_task_schema_draft() :: #atm_task_schema_draft{}.
-type atm_parallel_box_schema_draft() :: #atm_parallel_box_schema_draft{}.
-type atm_lane_schema_draft() :: #atm_lane_schema_draft{}.

-type atm_store_schema_draft() :: #atm_store_schema_draft{}.

-type atm_workflow_schema_revision_draft() :: #atm_workflow_schema_revision_draft{}.

-type atm_workflow_schema_dump_draft() :: #atm_workflow_schema_dump_draft{}.
-type atm_workflow_schema_dump() :: #atm_workflow_schema_dump{}.

-export_type([
    atm_openfaas_operation_spec_draft/0,
    atm_lambda_argument_spec_draft/0,
    atm_lambda_result_spec_draft/0,
    atm_lambda_revision_draft/0,

    atm_store_iterator_spec_draft/0,
    atm_task_argument_value_builder_draft/0,
    atm_task_schema_argument_mapper_draft/0,
    atm_task_schema_result_mapper_draft/0,
    atm_task_schema_draft/0,
    atm_parallel_box_schema_draft/0,
    atm_lane_schema_draft/0,

    atm_store_schema_draft/0,

    atm_workflow_schema_revision_draft/0,

    atm_workflow_schema_dump_draft/0,
    atm_workflow_schema_dump/0
]).

-type store_constraint() ::
    {type, automation:store_type()} |
    {data_spec, atm_data_spec:record()} |
    {supported_operation,
        ?ATM_PLACEHOLDER | atm_task_schema_result_mapper:dispatch_function(),
        ?ATM_PLACEHOLDER | atm_data_spec:record()}.

-record(creation_ctx, {
    workflow_schema_dump_draft :: atm_workflow_schema_dump:atm_workflow_schema_dump_draft(),

    lambdas = #{} :: #{automation:id() => #{
        atm_lambda_revision:revision_number() => atm_lambda_revision_draft()
    }},
    store_registry = #{} :: #{automation:id() => atm_store_schema_draft()},
    tasks_registry = #{} :: #{automation:id() => atm_task_schema_draft()},

    parallel_box_ids_per_lane = #{} :: #{automation:id() => [automation:id()]},
    task_ids_per_parallel_box = #{} :: #{automation:id() => [automation:id()]},
    task_id_to_lane_mapping = #{} :: #{automation:id() => automation:id()},

    store_constraints = #{} :: #{automation:id() => [store_constraint()]}
}).
-type creation_ctx() :: #creation_ctx{}.

-define(RAND_STR(), ?RAND_STR(16)).
-define(RAND_STR(Bytes), str_utils:rand_hex(Bytes)).
-define(RAND_BOOL(), lists_utils:random_element([true, false])).
-define(RAND_INT(From, To), From + rand:uniform(To - From + 1) - 1).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_from_draft(atm_workflow_schema_dump_draft()) -> atm_workflow_schema_dump().
create_from_draft(AtmWorkflowSchemaDumpDraft) ->
    CreationCtx0 = #creation_ctx{
        workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft
    },
    CreationCtx1 = lists:foldl(fun(Fun, CurrCreationCtx) ->
        Fun(CurrCreationCtx)
    end, CreationCtx0, [
        fun ensure_workflow_schema_revision_num/1,
        fun ensure_initial_workflow_schema_revision_draft/1,

        fun extract_lambdas_from_workflow_schema_dump_draft/1,
        fun extract_stores_from_workflow_schema_dump_draft/1,
        fun ensure_initial_lane_drafts/1,

        fun ensure_final_store_drafts/1,
        %% TODO ensure final lambdas and tasks

        fun assemble_final_atm_workflow_schema_dump_draft/1
    ]),
    draft_to_record(CreationCtx1#creation_ctx.workflow_schema_dump_draft).


-spec to_json(atm_workflow_schema_dump()) -> json_utils:json_map().
to_json(#atm_workflow_schema_dump{
    supplementary_lambdas = SupplementaryLambdas,
    revision_num = AtmWorkflowSchemaRevisionNum,
    revision = AtmWorkflowSchemaRevision
}) ->
    #{
        <<"name">> => atm_test_utils:example_name(),
        <<"summary">> => atm_test_utils:example_summary(),

        <<"revision">> => #{
            <<"originalRevisionNumber">> => AtmWorkflowSchemaRevisionNum,
            <<"atmWorkflowSchemaRevision">> => jsonable_record:to_json(
                AtmWorkflowSchemaRevision, atm_workflow_schema_revision
            ),
            <<"supplementaryAtmLambdas">> => maps:map(fun(_AtmLambdaId, AtmLambdaRevisionRegistry) ->
                jsonable_record:to_json(AtmLambdaRevisionRegistry, atm_lambda_revision_registry)
            end, SupplementaryLambdas)
        }
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_workflow_schema_revision_num(creation_ctx()) -> creation_ctx().
ensure_workflow_schema_revision_num(CreationCtx = #creation_ctx{
    workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft = #atm_workflow_schema_dump_draft{
        revision_num = PlaceholderOrRevisionNum
    }
}) ->
    CreationCtx#creation_ctx{
        workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft#atm_workflow_schema_dump_draft{
            revision_num = ensure_specified(PlaceholderOrRevisionNum, ?RAND_INT(1, 1000))
        }
    }.


%% @private
-spec ensure_initial_workflow_schema_revision_draft(creation_ctx()) -> creation_ctx().
ensure_initial_workflow_schema_revision_draft(CreationCtx = #creation_ctx{
    workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft = #atm_workflow_schema_dump_draft{
        revision = ?ATM_PLACEHOLDER
    }
}) ->
    ensure_initial_workflow_schema_revision_draft(CreationCtx#creation_ctx{
        workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft#atm_workflow_schema_dump_draft{
            revision = #atm_workflow_schema_revision_draft{}
        }
    });

ensure_initial_workflow_schema_revision_draft(CreationCtx = #creation_ctx{
    workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft = #atm_workflow_schema_dump_draft{
        revision = AtmWorkflowSchemaRevisionDraft = #atm_workflow_schema_revision_draft{
            description = PlaceholderOrDescription,
            state = PlaceholderOrState
        }
    }
}) ->
    CreationCtx#creation_ctx{
        workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft#atm_workflow_schema_dump_draft{
            revision = AtmWorkflowSchemaRevisionDraft#atm_workflow_schema_revision_draft{
                description = ensure_description(PlaceholderOrDescription),
                state = ensure_lifecycle_state(PlaceholderOrState)
            }
        }
    }.


%% @private
-spec extract_lambdas_from_workflow_schema_dump_draft(creation_ctx()) -> creation_ctx().
extract_lambdas_from_workflow_schema_dump_draft(CreationCtx0 = #creation_ctx{
    workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft = #atm_workflow_schema_dump_draft{
        supplementary_lambdas = PlaceholderOrAtmLambdas
    }
}) ->
    CreationCtx1 = CreationCtx0#creation_ctx{
        workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft#atm_workflow_schema_dump_draft{
            supplementary_lambdas = #{}
        }
    },

    maps:fold(fun(AtmLambdaId, PlaceholderOrAtmLambdaRevisionRegistryDraft, CreationCtx2) ->
        maps:fold(fun(AtmLambdaRevisionNum, PlaceholderOrAtmLambdaRevisionDraft, CreationCtx3) ->
            ensure_initial_lambda_revision_draft(
                AtmLambdaId, AtmLambdaRevisionNum, PlaceholderOrAtmLambdaRevisionDraft, CreationCtx3
            )
        end, CreationCtx2, ensure_specified(PlaceholderOrAtmLambdaRevisionRegistryDraft, #{}))
    end, CreationCtx1, ensure_specified(PlaceholderOrAtmLambdas, #{})).


%% @private
-spec extract_stores_from_workflow_schema_dump_draft(creation_ctx()) -> creation_ctx().
extract_stores_from_workflow_schema_dump_draft(CreationCtx0 = #creation_ctx{
    workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft = #atm_workflow_schema_dump_draft{
        revision = AtmWorkflowSchemaRevisionDraft = #atm_workflow_schema_revision_draft{
            stores = PlaceholderOrAtmStoreSchemaDrafts
        }
    }
}) ->
    CreationCtx1 = CreationCtx0#creation_ctx{
        workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft#atm_workflow_schema_dump_draft{
            revision = AtmWorkflowSchemaRevisionDraft#atm_workflow_schema_revision_draft{
                stores = []
            }
        }
    },

    lists:foldl(fun(PlaceholderOrAtmStoreSchemaDraft, CreationCtx2) ->
        ensure_initial_store_draft(PlaceholderOrAtmStoreSchemaDraft, CreationCtx2)
    end, CreationCtx1, ensure_specified(PlaceholderOrAtmStoreSchemaDrafts, [])).


%% @private
-spec ensure_initial_lane_drafts(creation_ctx()) -> creation_ctx().
ensure_initial_lane_drafts(CreationCtx0 = #creation_ctx{
    workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft = #atm_workflow_schema_dump_draft{
        revision = AtmWorkflowSchemaRevisionDraft = #atm_workflow_schema_revision_draft{
            lanes = PlaceholderOrAtmLaneSchemaDrafts
        }
    }
}) ->
    AtmLaneSchemaDrafts0 = ensure_specified(
        PlaceholderOrAtmLaneSchemaDrafts,
        lists:duplicate(?RAND_INT(1, 4), ?ATM_PLACEHOLDER)
    ),
    {AtmLaneSchemaDrafts1, CreationCtx1} = lists:mapfoldl(
        fun ensure_initial_lane_draft/2, CreationCtx0, AtmLaneSchemaDrafts0
    ),

    CreationCtx1#creation_ctx{
        workflow_schema_dump_draft = AtmWorkflowSchemaDumpDraft#atm_workflow_schema_dump_draft{
            revision = AtmWorkflowSchemaRevisionDraft#atm_workflow_schema_revision_draft{
                lanes = AtmLaneSchemaDrafts1
            }
        }
    }.


%% @private
-spec ensure_initial_lane_draft(?ATM_PLACEHOLDER | atm_lane_schema_draft(), creation_ctx()) ->
    {atm_lane_schema_draft(), creation_ctx()}.
ensure_initial_lane_draft(?ATM_PLACEHOLDER, CreationCtx) ->
    ensure_initial_lane_draft(#atm_lane_schema_draft{}, CreationCtx);

ensure_initial_lane_draft(AtmLaneSchemaDraft0 = #atm_lane_schema_draft{
    id = PlaceholderOrId,
    name = PlaceholderOrName,
    parallel_boxes = PlaceholderOrAtmParallelBoxDrafts,
    store_iterator_spec = PlaceholderOrAtmStoreIteratorSpec,
    max_retries = PlaceholderOrMaxRetries
}, CreationCtx0) ->
    Id = ensure_id(PlaceholderOrId),

    {AtmStoreIteratorSpecDraft, CreationCtx1} = ensure_initial_store_iterator_spec_draft(
        PlaceholderOrAtmStoreIteratorSpec, CreationCtx0
    ),
    IteratedStoreSchemaId = AtmStoreIteratorSpecDraft#atm_store_iterator_spec_draft.store_schema_id,

    {AtmParallelBoxSchemaDrafts, CreationCtx2} = ensure_initial_parallel_box_drafts(
        Id, IteratedStoreSchemaId, PlaceholderOrAtmParallelBoxDrafts, CreationCtx1
    ),

    AtmLaneSchemaDraft1 = AtmLaneSchemaDraft0#atm_lane_schema_draft{
        id = Id,
        name = ensure_name(PlaceholderOrName),

        parallel_boxes = AtmParallelBoxSchemaDrafts,

        store_iterator_spec = AtmStoreIteratorSpecDraft,
        max_retries = ensure_specified(PlaceholderOrMaxRetries, ?RAND_INT(1, 6))
    },

    {AtmLaneSchemaDraft1, CreationCtx2}.


%% @private
-spec ensure_initial_store_iterator_spec_draft(
    ?ATM_PLACEHOLDER | atm_store_iterator_spec_draft(),
    creation_ctx()
) ->
    {atm_store_iterator_spec_draft(), creation_ctx()}.
ensure_initial_store_iterator_spec_draft(?ATM_PLACEHOLDER, CreationCtx) ->
    ensure_initial_store_iterator_spec_draft(#atm_store_iterator_spec_draft{}, CreationCtx);

ensure_initial_store_iterator_spec_draft(AtmStoreIteratorSpecDraft0 = #atm_store_iterator_spec_draft{
    store_schema_id = PlaceholderOrAtmStoreSchemaId,
    max_batch_size = PlaceholderOrMaxBatchSize
}, CreationCtx0) ->
    AtmStoreSchemaId = ensure_id(PlaceholderOrAtmStoreSchemaId),
    AtmStoreIteratorSpecDraft1 = AtmStoreIteratorSpecDraft0#atm_store_iterator_spec_draft{
        store_schema_id = AtmStoreSchemaId,
        max_batch_size = ensure_specified(PlaceholderOrMaxBatchSize, ?RAND_INT(1, 10))
    },

    CreationCtx1 = add_initial_store_draft_if_not_already_specified(AtmStoreSchemaId, CreationCtx0),

    {AtmStoreIteratorSpecDraft1, CreationCtx1}.


%% @private
-spec ensure_initial_parallel_box_drafts(
    automation:id(),
    automation:id(),
    ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_parallel_box_schema_draft()],
    creation_ctx()
) ->
    {[atm_parallel_box_schema_draft()], creation_ctx()}.
ensure_initial_parallel_box_drafts(AtmLaneSchemaId, IteratedStoreSchemaId, ?ATM_PLACEHOLDER, CreationCtx) ->
    AtmParallelBoxSchemaDrafts = lists:duplicate(?RAND_INT(1, 3), ?ATM_PLACEHOLDER),
    ensure_initial_parallel_box_drafts(
        AtmLaneSchemaId, IteratedStoreSchemaId, AtmParallelBoxSchemaDrafts, CreationCtx
    );

ensure_initial_parallel_box_drafts(
    AtmLaneSchemaId,
    IteratedStoreSchemaId,
    AtmParallelBoxSchemaDrafts,
    CreationCtx0
) ->
    lists:mapfoldr(fun(AtmParallelBoxSchemaDraft, CreationCtx1) ->
        ensure_initial_parallel_box_draft(
            AtmLaneSchemaId, IteratedStoreSchemaId, AtmParallelBoxSchemaDraft, CreationCtx1
        )
    end, CreationCtx0, AtmParallelBoxSchemaDrafts).


%% @private
-spec ensure_initial_parallel_box_draft(
    automation:id(),
    automation:id(),
    ?ATM_PLACEHOLDER | atm_parallel_box_schema_draft(),
    creation_ctx()
) ->
    {atm_parallel_box_schema_draft(), creation_ctx()}.
ensure_initial_parallel_box_draft(AtmLaneSchemaId, IteratedStoreSchemaId, ?ATM_PLACEHOLDER, CreationCtx) ->
    ensure_initial_parallel_box_draft(
        AtmLaneSchemaId, IteratedStoreSchemaId, #atm_parallel_box_schema_draft{}, CreationCtx
    );

ensure_initial_parallel_box_draft(AtmLaneSchemaId, IteratedStoreSchemaId, #atm_parallel_box_schema_draft{
    id = PlaceholderOrId,
    name = PlaceholderOrName,
    tasks = PlaceholderOrAtmTaskSchemaDrafts
} = AtmParallelBoxSchemaDraft0, CreationCtx0) ->
    Id = ensure_id(PlaceholderOrId),
    AtmParallelBoxSchemaDraft1 = AtmParallelBoxSchemaDraft0#atm_parallel_box_schema_draft{
        id = Id,
        name = ensure_name(PlaceholderOrName),
        tasks = []
    },

    CreationCtx1 = ensure_initial_task_drafts(
        AtmLaneSchemaId, IteratedStoreSchemaId, Id, PlaceholderOrAtmTaskSchemaDrafts, CreationCtx0
    ),
    CreationCtx2 = CreationCtx1#creation_ctx{
        parallel_box_ids_per_lane = maps:update_with(AtmLaneSchemaId, fun(AtmParallelBoxIds) ->
            [Id | AtmParallelBoxIds]
        end, [Id], CreationCtx1#creation_ctx.parallel_box_ids_per_lane)
    },

    {AtmParallelBoxSchemaDraft1, CreationCtx2}.


%% @private
-spec ensure_initial_task_drafts(
    automation:id(),
    automation:id(),
    automation:id(),
    ?ATM_PLACEHOLDER | [?ATM_PLACEHOLDER | atm_task_schema_draft()],
    creation_ctx()
) ->
    creation_ctx().
ensure_initial_task_drafts(
    AtmLaneSchemaId,
    IteratedStoreSchemaId,
    AtmParallelBoxSchemaId,
    ?ATM_PLACEHOLDER,
    CreationCtx
) ->
    ensure_initial_task_drafts(
        AtmLaneSchemaId,
        IteratedStoreSchemaId,
        AtmParallelBoxSchemaId,
        lists:duplicate(?RAND_INT(1, 3), ?ATM_PLACEHOLDER),
        CreationCtx
    );

ensure_initial_task_drafts(
    AtmLaneSchemaId,
    IteratedStoreSchemaId,
    AtmParallelBoxSchemaId,
    AtmTaskSchemaDrafts,
    CreationCtx0
) ->
    lists:foldr(fun(AtmTaskSchemaDraft, CreationCtx1) ->
        ensure_initial_task_draft(
            AtmLaneSchemaId, IteratedStoreSchemaId, AtmParallelBoxSchemaId, AtmTaskSchemaDraft, CreationCtx1
        )
    end, CreationCtx0, AtmTaskSchemaDrafts).


%% @private
-spec ensure_initial_task_draft(
    automation:id(),
    automation:id(),
    automation:id(),
    ?ATM_PLACEHOLDER | atm_task_schema_draft(),
    creation_ctx()
) ->
    creation_ctx().
ensure_initial_task_draft(
    AtmLaneSchemaId,
    IteratedStoreSchemaId,
    AtmParallelBoxSchemaId,
    ?ATM_PLACEHOLDER,
    CreationCtx
) ->
    ensure_initial_task_draft(
        AtmLaneSchemaId, IteratedStoreSchemaId, AtmParallelBoxSchemaId, #atm_task_schema_draft{}, CreationCtx
    );

ensure_initial_task_draft(AtmLaneSchemaId, IteratedStoreSchemaId, AtmParallelBoxSchemaId, #atm_task_schema_draft{
    id = PlaceholderOrId,
    name = PlaceholderOrName,
    lambda_id = PlaceholderOrLambdaId,
    lambda_revision_number = PlaceholderOrLambdaRevisionNum,
    resource_spec_override = PlaceholderOrResourceSpecOverride
} = AtmTaskSchemaDraft0, CreationCtx0) ->
    Id = ensure_id(PlaceholderOrId),

    AtmLambdaId = ensure_id(PlaceholderOrLambdaId),
    AtmLambdaRevisionNum = ensure_specified(PlaceholderOrLambdaRevisionNum, ?RAND_INT(1, 100)),
    CreationCtx1 = add_initial_lambda_revision_draft_if_not_already_specified(
        AtmLambdaId, AtmLambdaRevisionNum, CreationCtx0
    ),
    AtmLambdaRevisionDraft = kv_utils:get(
        [AtmLambdaId, AtmLambdaRevisionNum], CreationCtx1#creation_ctx.lambdas
    ),

    AtmTaskSchemaDraft1 = AtmTaskSchemaDraft0#atm_task_schema_draft{
        id = Id,
        name = ensure_name(PlaceholderOrName),
        lambda_id = AtmLambdaId,
        lambda_revision_number = AtmLambdaRevisionNum,
        resource_spec_override = case PlaceholderOrResourceSpecOverride of
            ?ATM_PLACEHOLDER ->
                lists_utils:random_element([undefined | atm_test_utils:example_resource_specs()]);
            ResourceSpecOverride ->
                ResourceSpecOverride
        end
    },

    CreationCtx2 = add_initial_store_drafts_from_task_arg_mappers_if_not_already_specified(
        IteratedStoreSchemaId, AtmTaskSchemaDraft1, AtmLambdaRevisionDraft, CreationCtx1
    ),
    CreationCtx3 = add_initial_store_drafts_from_task_result_mappers_if_not_already_specified(
        AtmTaskSchemaDraft1, AtmLambdaRevisionDraft, CreationCtx2
    ),

    AtmTaskSchemaDraftsRegistry = CreationCtx3#creation_ctx.tasks_registry,
    AtmTaskSchemaIdsPerParallelBox = CreationCtx3#creation_ctx.task_ids_per_parallel_box,
    AtmTaskSchemaIdToLaneMapping = CreationCtx3#creation_ctx.task_id_to_lane_mapping,

    CreationCtx3#creation_ctx{
        tasks_registry = AtmTaskSchemaDraftsRegistry#{Id => AtmTaskSchemaDraft1},
        task_ids_per_parallel_box = maps:update_with(
            AtmParallelBoxSchemaId, fun(AtmTaskIds) -> [Id | AtmTaskIds] end, [Id], AtmTaskSchemaIdsPerParallelBox
        ),
        task_id_to_lane_mapping = AtmTaskSchemaIdToLaneMapping#{Id => AtmLaneSchemaId}
    }.


%% @private
-spec add_initial_store_drafts_from_task_arg_mappers_if_not_already_specified(
    automation:id(), atm_task_schema_draft(), atm_lambda_revision_draft(), creation_ctx()
) ->
    creation_ctx().
add_initial_store_drafts_from_task_arg_mappers_if_not_already_specified(
    _IteratedStoreSchemaId,
    #atm_task_schema_draft{argument_mappings = ?ATM_PLACEHOLDER},
    _AtmLambdaRevisionDraft,
    CreationCtx
) ->
    CreationCtx;

add_initial_store_drafts_from_task_arg_mappers_if_not_already_specified(
    IteratedStoreSchemaId,
    #atm_task_schema_draft{argument_mappings = ArgMappingDrafts},
    #atm_lambda_revision_draft{argument_specs = ArgSpecDrafts0},
    CreationCtx0
) ->
    ArgSpecDraftsMap = lists:foldl(fun
        (?ATM_PLACEHOLDER, Acc) ->
            Acc;
        (ArgSpecDraft = #atm_lambda_argument_spec_draft{name = ArgName}, Acc) ->
            Acc#{ArgName => ArgSpecDraft}
    end, #{}, ensure_specified(ArgSpecDrafts0, [])),

    lists:foldl(fun
        (#atm_task_schema_argument_mapper_draft{
            argument_name = ArgName,
            value_builder = #atm_task_argument_value_builder_draft{type = iterated_item}
        }, CreationCtx1) ->
            case maps:get(ArgName, ArgSpecDraftsMap, ?ATM_PLACEHOLDER) of
                #atm_lambda_argument_spec_draft{data_spec = AtmDataSpec} when AtmDataSpec /= ?ATM_PLACEHOLDER ->
                    register_store_constraints(
                        IteratedStoreSchemaId, [{data_spec, AtmDataSpec}], CreationCtx1
                    );
                _ ->
                    CreationCtx1
            end;

        (#atm_task_schema_argument_mapper_draft{
            argument_name = ArgName,
            value_builder = #atm_task_argument_value_builder_draft{
                type = single_value_store_content,
                recipe = AtmStoreSchemaId
            }
        }, CreationCtx1) when is_binary(AtmStoreSchemaId) ->
            Constraints0 = [{type, single_value}],
            Constraints1 = case maps:get(ArgName, ArgSpecDraftsMap, ?ATM_PLACEHOLDER) of
                #atm_lambda_argument_spec_draft{data_spec = AtmDataSpec} when AtmDataSpec /= ?ATM_PLACEHOLDER ->
                    [{data_spec, AtmDataSpec} | Constraints0];
                _ ->
                    Constraints0
            end,
            CreationCtx2 = register_store_constraints(AtmStoreSchemaId, Constraints1, CreationCtx1),
            add_initial_store_draft_if_not_already_specified(AtmStoreSchemaId, CreationCtx2);

        (_, CreationCtx1) ->
            CreationCtx1
    end, CreationCtx0, ArgMappingDrafts).


%% @private
-spec add_initial_store_drafts_from_task_result_mappers_if_not_already_specified(
    atm_task_schema_draft(), atm_lambda_revision_draft(), creation_ctx()
) ->
    creation_ctx().
add_initial_store_drafts_from_task_result_mappers_if_not_already_specified(
    #atm_task_schema_draft{result_mappings = ?ATM_PLACEHOLDER},
    _AtmLambdaRevisionDraft,
    CreationCtx
) ->
    CreationCtx;

add_initial_store_drafts_from_task_result_mappers_if_not_already_specified(
    #atm_task_schema_draft{result_mappings = ResultMappingDrafts},
    #atm_lambda_revision_draft{resource_spec = ResultSpecDrafts0},
    CreationCtx0
) ->
    ResultSpecDraftsMap = lists:foldl(fun
        (?ATM_PLACEHOLDER, Acc) ->
            Acc;
        (ResultSpecDraft = #atm_lambda_result_spec_draft{name = ResultName}, Acc) ->
            Acc#{ResultName => ResultSpecDraft}
    end, #{}, ensure_specified(ResultSpecDrafts0, [])),

    lists:foldl(fun
        (#atm_task_schema_result_mapper_draft{
            result_name = ResultName,
            dispatch_function = DispatchFunction,
            store_schema_id = AtmStoreSchemaId
        }, CreationCtx1) when is_binary(AtmStoreSchemaId) ->
            ResultAtmDataSpec = case maps:get(ResultName, ResultSpecDraftsMap, ?ATM_PLACEHOLDER) of
                #atm_lambda_result_spec_draft{data_spec = AtmDataSpec} -> AtmDataSpec;
                _ -> ?ATM_PLACEHOLDER
            end,
            CreationCtx2 = case {DispatchFunction, ResultAtmDataSpec} of
                {?ATM_PLACEHOLDER, ?ATM_PLACEHOLDER} ->
                    CreationCtx1;
                _ ->
                    register_store_constraints(
                        AtmStoreSchemaId,
                        [{supported_operation, DispatchFunction, ResultAtmDataSpec}],
                        CreationCtx1
                    )
            end,
            add_initial_store_draft_if_not_already_specified(AtmStoreSchemaId, CreationCtx2);

        (_, CreationCtx1) ->
            CreationCtx1
    end, CreationCtx0, ResultMappingDrafts).


%% @private
-spec register_store_constraints(automation:id(), [store_constraint()], creation_ctx()) ->
    creation_ctx().
register_store_constraints(AtmStoreSchemaId, NewConstraints, CreationCtx = #creation_ctx{
    store_constraints = AtmStoreConstraints
}) ->
    CreationCtx#creation_ctx{store_constraints = maps:update_with(
        AtmStoreSchemaId,
        fun(PrevConstraints) -> lists:usort(NewConstraints ++ PrevConstraints) end,
        NewConstraints,
        AtmStoreConstraints
    )}.


%% @private
-spec add_initial_lambda_revision_draft_if_not_already_specified(
    automation:id(),
    atm_lambda_revision:revision_number(),
    creation_ctx()
) ->
    creation_ctx().
add_initial_lambda_revision_draft_if_not_already_specified(
    AtmLambdaId,
    AtmLambdaRevisionNum,
    CreationCtx = #creation_ctx{lambdas = AtmLambdaDrafts}
) ->
    case kv_utils:find([AtmLambdaId, AtmLambdaRevisionNum], AtmLambdaDrafts) of
        {ok, _} ->
            CreationCtx;
        error ->
            ensure_initial_lambda_revision_draft(
                AtmLambdaId, AtmLambdaRevisionNum, #atm_lambda_revision_draft{}, CreationCtx
            )
    end.


%% @private
-spec ensure_initial_lambda_revision_draft(
    automation:id(),
    atm_lambda_revision:revision_number(),
    ?ATM_PLACEHOLDER | atm_lambda_revision_draft(),
    creation_ctx()
) ->
    creation_ctx().
ensure_initial_lambda_revision_draft(AtmLambdaId, AtmLambdaRevisionNum, ?ATM_PLACEHOLDER, CreationCtx) ->
    ensure_initial_lambda_revision_draft(
        AtmLambdaId, AtmLambdaRevisionNum, #atm_lambda_revision_draft{}, CreationCtx
    );

ensure_initial_lambda_revision_draft(
    AtmLambdaId,
    AtmLambdaRevisionNum,
    AtmLambdaRevisionDraft0 = #atm_lambda_revision_draft{
        name = PlaceholderOrName,
        summary = PlaceholderOrSummary,
        description = PlaceholderOrDescription,

        operation_spec = PlaceholderOrOperationSpec,

        preferred_batch_size = PlaceholderOrPreferredBatchSize,
        resource_spec = PlaceholderOrResourceSpec,
        state = PlaceholderOrState
    },
    CreationCtx = #creation_ctx{lambdas = AtmLambdaDrafts}
) ->
    AtmLambdaRevisionDraft1 = AtmLambdaRevisionDraft0#atm_lambda_revision_draft{
        name = ensure_name(PlaceholderOrName),
        summary = ensure_summary(PlaceholderOrSummary),
        description = ensure_description(PlaceholderOrDescription),
        operation_spec = ensure_specified(
            PlaceholderOrOperationSpec, atm_test_utils:example_operation_spec()
        ),
        preferred_batch_size = ensure_specified(PlaceholderOrPreferredBatchSize, ?RAND_INT(1, 100)),
        resource_spec = ensure_specified(
            PlaceholderOrResourceSpec, lists_utils:random_element(atm_test_utils:example_resource_specs())
        ),
        state = ensure_lifecycle_state(PlaceholderOrState)
    },

    CreationCtx#creation_ctx{lambdas = maps:update_with(
        AtmLambdaId,
        fun(AtmLambdaRevisionRegistryDraft) ->
            AtmLambdaRevisionRegistryDraft#{AtmLambdaRevisionNum => AtmLambdaRevisionDraft1}
        end,
        AtmLambdaDrafts,
        #{AtmLambdaRevisionNum => AtmLambdaRevisionDraft1}
    )}.


%% @private
-spec add_initial_store_draft_if_not_already_specified(automation:id(), creation_ctx()) ->
    creation_ctx().
add_initial_store_draft_if_not_already_specified(?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, CreationCtx) ->
    CreationCtx;

add_initial_store_draft_if_not_already_specified(?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, CreationCtx) ->
    CreationCtx;

add_initial_store_draft_if_not_already_specified(AtmStoreSchemaId, CreationCtx) ->
    case maps:is_key(AtmStoreSchemaId, CreationCtx#creation_ctx.store_registry) of
        true ->
            CreationCtx;
        false ->
            ensure_initial_store_draft(#atm_store_schema_draft{id = AtmStoreSchemaId}, CreationCtx)
    end.


%% @private
-spec ensure_initial_store_draft(?ATM_PLACEHOLDER | atm_store_schema_draft(), creation_ctx()) ->
    creation_ctx().
ensure_initial_store_draft(?ATM_PLACEHOLDER, CreationCtx) ->
    ensure_initial_store_draft(#atm_store_schema_draft{}, CreationCtx);

ensure_initial_store_draft(AtmStoreSchemaDraft = #atm_store_schema_draft{
    id = PlaceholderOrId,
    name = PlaceholderOrName,
    description = PlaceholderOrDescription
}, CreationCtx) ->
    Id = ensure_id(PlaceholderOrId),
    AtmStoresRegistry = CreationCtx#creation_ctx.store_registry,

    CreationCtx#creation_ctx{
        store_registry = AtmStoresRegistry#{Id => AtmStoreSchemaDraft#atm_store_schema_draft{
            id = Id,
            name = ensure_name(PlaceholderOrName),
            description = ensure_description(PlaceholderOrDescription)
        }}
    }.


%% @private
-spec ensure_final_store_drafts(creation_ctx()) -> creation_ctx().
ensure_final_store_drafts(CreationCtx = #creation_ctx{store_registry = AtmStoreSchemaDraftsRegistry}) ->
    CreationCtx#creation_ctx{
        store_registry = maps:map(fun(_AtmStoreSchemaId, AtmStoreSchemaDraft) ->
            ensure_final_store_draft(AtmStoreSchemaDraft, CreationCtx)
        end, AtmStoreSchemaDraftsRegistry)
    }.


%% @private
-spec ensure_final_store_draft(atm_store_schema_draft(), creation_ctx()) ->
    atm_store_schema_draft().
ensure_final_store_draft(AtmStoreSchemaDraft = #atm_store_schema_draft{
    id = Id,
    type = PlaceholderOrType,
    data_spec = PlaceholderOrAtmDataSpec,
    requires_initial_value = PlaceholderOrRequiresInitialValue,
    default_initial_value = PlaceholderOrDefaultInitialValue
}, CreationCtx) ->
    {Type, AtmDataSpec} = infer_store_type_and_data_spec(
        PlaceholderOrType, PlaceholderOrAtmDataSpec, maps:get(Id, CreationCtx#creation_ctx.store_constraints)
    ),

    RequiresInitialValue = case {PlaceholderOrRequiresInitialValue, atm_data_spec:get_type(AtmDataSpec)} of
        {?ATM_PLACEHOLDER, atm_file_type} -> false;
        {?ATM_PLACEHOLDER, atm_dataset_type} -> false;
        {?ATM_PLACEHOLDER, _} -> ?RAND_BOOL();
        _ -> PlaceholderOrRequiresInitialValue
    end,

    AtmStoreSchemaDraft#atm_store_schema_draft{
        type = Type,
        data_spec = AtmDataSpec,
        requires_initial_value = RequiresInitialValue,
        default_initial_value = ensure_default_initial_value(
            PlaceholderOrDefaultInitialValue, Type, AtmDataSpec
        )
    }.


%% @private
-spec infer_store_type_and_data_spec(
    ?ATM_PLACEHOLDER | automation:store_type(),
    ?ATM_PLACEHOLDER | atm_data_spec:record(),
    [store_constraint()]
) ->
    {automation:store_type(), atm_data_spec:record()}.
infer_store_type_and_data_spec(PlaceholderOrType, PlaceholderOrAtmDataSpec, Constraints) ->
    FinalConstraints = lists:foldl(fun
        ({type, Type}, Acc) ->
            Acc#{types => sets:intersection(sets:from_list([Type]), maps:get(types, Acc))};

        ({data_spec, AtmDataSpec}, Acc) ->
            Acc#{data_spec => AtmDataSpec};

        ({supported_operation, extend, #atm_data_spec{type = atm_array_type} = AtmDataSpec}, Acc) ->
            PossibleStoreTypes = get_store_types_by_supported_operation(extend),

            Acc#{
                types => sets:intersection(PossibleStoreTypes, maps:get(types, Acc)),
                data_spec => maps:get(item_data_spec, AtmDataSpec#atm_data_spec.value_constraints)
            };

        ({supported_operation, DispatchFun, AtmDataSpec}, Acc0) ->
            PossibleStoreTypes = get_store_types_by_supported_operation(DispatchFun),
            Acc1 = Acc0#{types => sets:intersection(PossibleStoreTypes, maps:get(types, Acc0))},

            case AtmDataSpec of
                ?ATM_PLACEHOLDER -> Acc1;
                _ -> Acc1#{data_spec => AtmDataSpec}
            end
    end, #{types => get_store_types_by_supported_operation(?ATM_PLACEHOLDER)}, Constraints),

    PossibleStoreTypes = case PlaceholderOrType of
        ?ATM_PLACEHOLDER ->
            utils:ensure_defined(
                sets:to_list(maps:get(types, FinalConstraints)),
                [],
                get_store_types_by_supported_operation(?ATM_PLACEHOLDER)
            );
        _ ->
            [PlaceholderOrType]
    end,
    ItemAtmDataSpecOrPlaceholder = case PlaceholderOrAtmDataSpec of
        ?ATM_PLACEHOLDER -> maps:get(data_spec, FinalConstraints, ?ATM_PLACEHOLDER);
        _ -> PlaceholderOrAtmDataSpec
    end,


    StoreType = PlaceholderOrType,  %% TODO
    ItemAtmDataSpec = PlaceholderOrAtmDataSpec,  %% TODO


    {StoreType, ItemAtmDataSpec}.


%% @private
-spec get_store_types_by_supported_operation(
    ?ATM_PLACEHOLDER | atm_task_schema_result_mapper:dispatch_function()
) ->
    sets:set(automation:store_type()).
get_store_types_by_supported_operation(set) ->
    sets:from_list([single_value]);

get_store_types_by_supported_operation(append) ->
    sets:from_list([audit_log, list, tree_forest]);

get_store_types_by_supported_operation(extend) ->
    sets:from_list([audit_log, list, tree_forest]);

get_store_types_by_supported_operation(?ATM_PLACEHOLDER) ->
    sets:from_list([audit_log, list, range, single_value, tree_forest]).


%% @private
-spec ensure_default_initial_value(
    ?ATM_PLACEHOLDER | undefined | json_utils:json_term(),
    automation:store_type(),
    atm_data_spec:record()
) ->
    undefined | json_utils:json_term().
ensure_default_initial_value(?ATM_PLACEHOLDER, range, _) ->
    lists_utils:random_element([
        undefined,
        #{<<"start">> => ?RAND_INT(0, 10), <<"end">> => ?RAND_INT(100, 200), <<"step">> => ?RAND_INT(0, 5)}
    ]);

ensure_default_initial_value(?ATM_PLACEHOLDER, single_value, AtmDataSpec) ->
    lists_utils:random_element([undefined, atm_test_utils:example_initial_value(AtmDataSpec)]);

ensure_default_initial_value(?ATM_PLACEHOLDER, _, AtmDataSpec) ->
    case ?RAND_BOOL() of
        true ->
            undefined;
        false ->
            lists:filtermap(fun(_) ->
                case atm_test_utils:example_initial_value(AtmDataSpec) of
                    undefined -> false;
                    Value -> {true, Value}
                end
            end, lists:seq(1, ?RAND_INT(50, 150)))
    end;

ensure_default_initial_value(DefaultInitialValue, _, _) ->
    DefaultInitialValue.


%% @private
-spec assemble_final_atm_workflow_schema_dump_draft(creation_ctx()) -> creation_ctx().
assemble_final_atm_workflow_schema_dump_draft(CreationCtx = #creation_ctx{
    workflow_schema_dump_draft = AtmWorkflowSchemaDraft = #atm_workflow_schema_dump_draft{
        revision = AtmWorkflowSchemaRevisionDraft = #atm_workflow_schema_revision_draft{
            lanes = AtmLaneSchemaDrafts0
        }
    },
    lambdas = AtmLambdaDrafts,
    store_registry = AtmStoresRegistry,
    tasks_registry = AtmTasksRegistry,
    task_ids_per_parallel_box = AtmTaskSchemaIdsPerParallelBox
}) ->
    AtmLaneSchemaDrafts1 = lists:map(fun(AtmLaneSchemaDraft) ->
        AtmLaneSchemaDraft#atm_lane_schema_draft{
            parallel_boxes = lists:map(fun(AtmParallelBoxSchemaDraft0) ->
                AtmParallelBoxSchemaId = AtmParallelBoxSchemaDraft0#atm_parallel_box_schema_draft.id,

                AtmParallelBoxSchemaDraft0#atm_parallel_box_schema_draft{
                    tasks = lists:map(fun(AtmTaskSchemaId) ->
                        maps:get(AtmTaskSchemaId, AtmTasksRegistry)
                    end, maps:get(AtmParallelBoxSchemaId, AtmTaskSchemaIdsPerParallelBox))
                }
            end, AtmLaneSchemaDraft#atm_lane_schema_draft.parallel_boxes)
        }
    end, AtmLaneSchemaDrafts0),

    CreationCtx#creation_ctx{workflow_schema_dump_draft = AtmWorkflowSchemaDraft#atm_workflow_schema_dump_draft{
        revision = AtmWorkflowSchemaRevisionDraft#atm_workflow_schema_revision_draft{
            stores = maps:values(AtmStoresRegistry),
            lanes = AtmLaneSchemaDrafts1
        },
        supplementary_lambdas = AtmLambdaDrafts
    }}.


%% @private
-spec draft_to_record
    (atm_workflow_schema_dump_draft()) -> atm_workflow_schema_dump();
    (atm_lambda_revision_draft()) -> atm_lambda_revision:record();
    (atm_openfaas_operation_spec_draft()) -> atm_openfaas_operation_spec:record();
    (atm_lambda_argument_spec_draft()) -> atm_lambda_argument_spec:record();
    (atm_lambda_result_spec_draft()) -> atm_lambda_result_spec:record();
    (atm_workflow_schema_revision_draft()) -> atm_workflow_schema_revision:record();
    (atm_store_schema_draft()) -> atm_store_schema:record();
    (atm_lane_schema_draft()) -> atm_lane_schema:record();
    (atm_parallel_box_schema_draft()) -> atm_parallel_box_schema:record();
    (atm_task_schema_draft()) -> atm_task_schema:record();
    (atm_task_schema_argument_mapper_draft()) -> atm_task_schema_argument_mapper:record();
    (atm_task_argument_value_builder_draft()) -> atm_task_argument_value_builder:record();
    (atm_task_schema_result_mapper_draft()) -> atm_task_schema_result_mapper:record();
    (atm_store_iterator_spec_draft()) -> atm_store_iterator_spec:record().
draft_to_record(#atm_workflow_schema_dump_draft{
    supplementary_lambdas = SupplementaryLambdaDrafts,
    revision_num = AtmWorkflowSchemaRevisionNum,
    revision = AtmWorkflowSchemaRevision
}) ->
    SupplementaryLambdas = maps:map(fun(_AtmLambdaId, AtmLambdaRevisionRegistryDraft) ->
        #atm_lambda_revision_registry{registry = maps:map(fun(_RevisionNum, AtmLambdaRevisionDraft) ->
            draft_to_record(AtmLambdaRevisionDraft)
        end, AtmLambdaRevisionRegistryDraft)}
    end, SupplementaryLambdaDrafts),

    #atm_workflow_schema_dump{
        supplementary_lambdas = SupplementaryLambdas,
        revision_num = AtmWorkflowSchemaRevisionNum,
        revision = draft_to_record(AtmWorkflowSchemaRevision)
    };

draft_to_record(#atm_lambda_revision_draft{
    name = Name,
    summary = Summary,
    description = Description,

    operation_spec = OperationSpecDraft,
    argument_specs = ArgSpecDrafts,
    result_specs = ResultSpecDrafts,

    preferred_batch_size = PreferredBatchSize,
    resource_spec = ResourceSpec,
    state = State
}) ->
    AtmLambdaRevision = #atm_lambda_revision{
        name = Name,
        summary = Summary,
        description = Description,

        operation_spec = draft_to_record(OperationSpecDraft),
        argument_specs = lists:map(fun draft_to_record/1, ArgSpecDrafts),
        result_specs = lists:map(fun draft_to_record/1, ResultSpecDrafts),

        preferred_batch_size = PreferredBatchSize,
        resource_spec = ResourceSpec,
        state = State,
        checksum = <<>>
    },

    AtmLambdaRevision#atm_lambda_revision{
        checksum = atm_lambda_revision:calculate_checksum(AtmLambdaRevision)
    };

draft_to_record(#atm_openfaas_operation_spec_draft{
    docker_image = DockerImage,
    docker_execution_options = DockerExecutionOptions
}) ->
    #atm_openfaas_operation_spec{
        docker_image = DockerImage,
        docker_execution_options = DockerExecutionOptions
    };

draft_to_record(#atm_lambda_argument_spec_draft{
    name = Name,
    data_spec = AtmDataSpec,
    is_optional = IsOptional,
    default_value = DefaultValue
}) ->
    #atm_lambda_argument_spec{
        name = Name,
        data_spec = AtmDataSpec,
        is_optional = IsOptional,
        default_value = DefaultValue
    };

draft_to_record(#atm_lambda_result_spec_draft{
    name = Name,
    data_spec = AtmDataSpec
}) ->
    #atm_lambda_result_spec{
        name = Name,
        data_spec = AtmDataSpec
    };

draft_to_record(#atm_workflow_schema_revision_draft{
    description = Description,
    stores = AtmStoreSchemaDrafts,
    lanes = AtmLaneSchemaDrafts,
    state = State
}) ->
    #atm_workflow_schema_revision{
        description = Description,
        stores = lists:map(fun draft_to_record/1, AtmStoreSchemaDrafts),
        lanes = lists:map(fun draft_to_record/1, AtmLaneSchemaDrafts),
        state = State
    };

draft_to_record(#atm_store_schema_draft{
    id = Id,
    name = Name,
    description = Description,
    type = Type,
    data_spec = AtmDataSpec,
    requires_initial_value = RequiresInitialValue,
    default_initial_value = DefaultInitialValue
}) ->
    #atm_store_schema{
        id = Id,
        name = Name,
        description = Description,
        type = Type,
        data_spec = AtmDataSpec,
        requires_initial_value = RequiresInitialValue,
        default_initial_value = DefaultInitialValue
    };

draft_to_record(#atm_lane_schema_draft{
    id = Id,
    name = Name,

    parallel_boxes = AtmParallelBoxSchemaDrafts,

    store_iterator_spec = AtmStoreIteratorSpecDraft,
    max_retries = MaxRetries
}) ->
    #atm_lane_schema{
        id = Id,
        name = Name,

        parallel_boxes = lists:map(fun draft_to_record/1, AtmParallelBoxSchemaDrafts),

        store_iterator_spec = draft_to_record(AtmStoreIteratorSpecDraft),
        max_retries = MaxRetries
    };

draft_to_record(#atm_parallel_box_schema_draft{
    id = Id,
    name = Name,
    tasks = AtmTaskSchemaDrafts
}) ->
    #atm_parallel_box_schema{
        id = Id,
        name = Name,
        tasks = lists:map(fun draft_to_record/1, AtmTaskSchemaDrafts)
    };

draft_to_record(#atm_task_schema_draft{
    id = Id,
    name = Name,
    lambda_id = AtmLambdaId,
    lambda_revision_number = AtmLambdaRevisionNum,

    argument_mappings = ArgMappingDrafts,
    result_mappings = ResultMappingDrafts,

    resource_spec_override = ResourceSpecOverride
}) ->
    #atm_task_schema{
        id = Id,
        name = Name,
        lambda_id = AtmLambdaId,
        lambda_revision_number = AtmLambdaRevisionNum,

        argument_mappings = lists:map(fun draft_to_record/1, ArgMappingDrafts),
        result_mappings = lists:map(fun draft_to_record/1, ResultMappingDrafts),

        resource_spec_override = ResourceSpecOverride
    };

draft_to_record(#atm_task_schema_argument_mapper_draft{
    argument_name = ArgName,
    value_builder = ValueBuilderDraft
}) ->
    #atm_task_schema_argument_mapper{
        argument_name = ArgName,
        value_builder = draft_to_record(ValueBuilderDraft)
    };

draft_to_record(#atm_task_argument_value_builder_draft{type = Type, recipe = Recipe}) ->
    #atm_task_argument_value_builder{type = Type, recipe = Recipe};

draft_to_record(#atm_task_schema_result_mapper_draft{
    result_name = ResultName,
    store_schema_id = AtmStoreSchemaId,
    dispatch_function = DispatchFunction
}) ->
    #atm_task_schema_result_mapper{
        result_name = ResultName,
        store_schema_id = AtmStoreSchemaId,
        dispatch_function = DispatchFunction
    };

draft_to_record(#atm_store_iterator_spec_draft{
    store_schema_id = AtmStoreSchemaId,
    max_batch_size = MaxBatchSize
}) ->
    #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId,
        max_batch_size = MaxBatchSize
    }.


%% @private
-spec ensure_specified(Value, DefaultValue) -> Value | DefaultValue.
ensure_specified(?ATM_PLACEHOLDER, Default) -> Default;
ensure_specified(Value, _Default) -> Value.


%% @private
-spec ensure_id(?ATM_PLACEHOLDER | automation:id()) -> automation:id().
ensure_id(?ATM_PLACEHOLDER) -> atm_test_utils:example_id();
ensure_id(Id) -> Id.


%% @private
-spec ensure_name(?ATM_PLACEHOLDER | automation:name()) -> automation:name().
ensure_name(?ATM_PLACEHOLDER) -> atm_test_utils:example_name();
ensure_name(Name) -> Name.


%% @private
-spec ensure_summary(?ATM_PLACEHOLDER | automation:summary()) ->
    automation:summary().
ensure_summary(?ATM_PLACEHOLDER) -> atm_test_utils:example_summary();
ensure_summary(Description) -> Description.


%% @private
-spec ensure_description(?ATM_PLACEHOLDER | automation:description()) ->
    automation:description().
ensure_description(?ATM_PLACEHOLDER) -> atm_test_utils:example_description();
ensure_description(Description) -> Description.


%% @private
-spec ensure_lifecycle_state(?ATM_PLACEHOLDER | json_utils:json_term()) ->
    json_utils:json_term().
ensure_lifecycle_state(?ATM_PLACEHOLDER) -> atm_test_utils:example_lifecycle_state();
ensure_lifecycle_state(State) -> State.
