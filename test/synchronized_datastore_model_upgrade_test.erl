%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for verifying if any datastore model has been upgraded.
%%% @end
%%%--------------------------------------------------------------------
-module(synchronized_datastore_model_upgrade_test).
-author("Katarzyna Such").

-include_lib("eunit/include/eunit.hrl").

-define(DATASTORE_MODALS, [
    {file_meta, 12},
    {file_location, 6},
    {custom_metadata, 3},
    {times, 1},
    {space_transfer_stats, 1},
    {transfer, 12},
    {transferred_file, 2},
    {replica_deletion, 3},
    {index, 1},
    {tree_traverse_job, 6},
    {qos_entry, 2},
    {dataset, 2},
    {archive, 1},
    {archive_recall_details, 1}
]).

datastore_model_version_verification_test_() ->
    Results = lists:map(fun(Model) ->
        ModelsCtxNotExported = [qos_entry],
        Ctx = case erlang:function_exported(Model, get_ctx, 0) of
            true ->
                Model:get_ctx();
            false ->
                case lists:member(Model, ModelsCtxNotExported) of
                    true ->
                        json_infinite_log_model:get_ctx();
                    false ->
                        #{}
                end
        end,
        case maps:is_key(sync_enabled, Ctx) of
                true ->
                    ActualRecordVersion =
                        try
                            Model:get_record_version()
                        catch
                            error:undef ->
                                1
                        end,
                    ExpectedRecordVersion = proplists:get_value(Model, ?DATASTORE_MODALS),
                    {ok, Model, ActualRecordVersion, ExpectedRecordVersion};
                false ->
                    sync_not_enabled
        end

    end, datastore_config_plugin:get_models()),
    FilteredResults = lists:filter(fun(Result)->
       case Result of
           sync_not_enabled -> false;
           _ -> true
       end
    end, Results),
    lists:map(fun({ok, _, ActualRecordVersion, ExpectedRecordVersion})->
        ?_assertEqual(ActualRecordVersion, ExpectedRecordVersion)
    end, FilteredResults).

check_new_datastore_models_test() ->
    Models = datastore_config_plugin:get_models(),
    ModelsWithCtx= lists:filter(fun(Model) -> erlang:function_exported(Model, get_ctx, 0) end, Models),
    SelectedModels = lists:filter(fun(Model)->
        Ctx = Model:get_ctx(),
        maps:is_key(sync_enabled, Ctx)
    end, ModelsWithCtx),
    lists:foreach(fun(Model) ->
        case lists:member(Model, proplists:get_keys(?DATASTORE_MODALS)) of
            false ->
                ?assert(false, "New datastore model found: " ++ atom_to_list(Model));
            true ->
                ok
        end
    end, SelectedModels).

check_missing_datastore_models_test() ->
    lists:foreach(fun(Model) ->
        case lists:member(Model, datastore_config_plugin:get_models()) of
            false ->
                ?assert(false, "Missing datastore model: " ++ atom_to_list(Model));
            true ->
                ok
        end
    end, proplists:get_keys(?DATASTORE_MODALS)).
