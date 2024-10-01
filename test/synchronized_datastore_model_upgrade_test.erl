%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests that keep track if any datastore model that is synchronized
%%% between providers has been upgraded (i.e. the record version has changed).
%%% Such upgrades are disallowed between minor system versions, as they break
%%% the compatibility of providers. They can only be introduced when releasing
%%% a new major version.
%%% @end
%%%--------------------------------------------------------------------
-module(synchronized_datastore_model_upgrade_test).
-author("Katarzyna Such").

-include_lib("eunit/include/eunit.hrl").

%% The map of all synchronized models and their versions.
%% If a version is changed, an adjustment to this map will be required
%% for the tests to pass. The code author and reviewers **must make sure**
%% that the change can be introduced (e.g. a new model is added or a new major
%% version is to be released).
-define(FROZEN_MODEL_VERSIONS, #{
    archive => 1,
    archive_recall_details => 1,
    custom_metadata => 3,
    dataset => 2,
    file_location => 6,
    file_meta => 12,
    index => 1,
    qos_entry => 2,
    replica_deletion => 3,
    space_transfer_stats => 1,
    times => 2,
    transfer => 12,
    transferred_file => 2,
    tree_traverse_job => 6
}).


datastore_model_version_verification_test_() ->
    ActualModelVersions = lists:filtermap(fun(Model) ->
        Ctx = get_model_ctx(Model),
        case is_sync_enabled(Ctx) of
            true -> {true, {Model, get_model_version(Model)}};
            false -> false
        end
    end, datastore_config_plugin:get_models()),

    ?assertEqual(
        lists:sort(proplists:get_keys(ActualModelVersions)),
        lists:sort(maps:keys(?FROZEN_MODEL_VERSIONS))
    ),

    lists:map(fun({Model, Version}) ->
        {str_utils:to_binary(Model), ?_assertEqual(Version, maps:get(Model, ?FROZEN_MODEL_VERSIONS, unknown))}
    end, ActualModelVersions).


get_model_ctx(qos_entry_audit_log) ->
    json_infinite_log_model:get_ctx();
get_model_ctx(Model) ->
    datastore_model_default:get_ctx(Model).


is_sync_enabled(#{sync_enabled := true}) -> true;
is_sync_enabled(_) -> false.


get_model_version(Model) ->
    try
        Model:get_record_version()
    catch
        error:undef ->
            1
    end.
