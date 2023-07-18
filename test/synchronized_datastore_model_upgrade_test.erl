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


-define(DATASTORE_MODELS, #{
    file_meta => 12,
    file_location => 6,
    custom_metadata => 3,
    times => 1,
    space_transfer_stats => 1,
    transfer => 12,
    transferred_file => 2,
    replica_deletion => 3,
    index => 1,
    tree_traverse_job => 6,
    qos_entry => 2,
    dataset => 2,
    archive => 1,
    archive_recall_details => 1
}).


datastore_model_version_verification_test_() ->
    ActualModelVersions = lists:filtermap(fun(Model) ->
        Ctx = get_model_ctx(Model),
        case is_sync_enabled(Ctx) of
            true -> {true, {Model, get_model_version(Model)}};
            false -> false
        end
    end, datastore_config_plugin:get_models()),

    ?assertEqual(lists:sort(maps:keys(maps:from_list(ActualModelVersions))), lists:sort(maps:keys(?DATASTORE_MODELS))),

    lists:map(fun({Model, Version}) ->
        ?_assertEqual({Model, Version}, {Model, maps:get(Model, ?DATASTORE_MODELS)})
    end, ActualModelVersions).


get_model_ctx(qos_entry_audit_log) ->
    json_infinite_log_model:get_ctx();
get_model_ctx(Model) ->
    case erlang:function_exported(Model, get_ctx, 0) of
        true ->
            Model:get_ctx();
        false ->
            #{}
    end.


is_sync_enabled(#{sync_enabled := true}) -> true;
is_sync_enabled(_) -> false.


get_model_version(Model) ->
    try
        Model:get_record_version()
    catch
        error:undef ->
            1
    end.
