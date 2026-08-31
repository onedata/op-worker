%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% High-level unified API (to be used by REST?GUI) to describe storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_describer).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").

%% API
-export([
    describe/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec describe(storage:id()) -> {ok, onedata_storage:description()} | errors:error().
describe(StorageId) ->
    try
        do_describe(StorageId)
    catch Class:Reason:Stacktrace ->
        ?examine_exception(
            "Unexpected error when describing storage '~ts'", [StorageId],
            Class, Reason, Stacktrace
        )
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec do_describe(storage:id()) ->
    {ok, onedata_storage:description()} | errors:error().
do_describe(StorageId) ->
    case storage:get(StorageId) of
        {ok, StorageData} ->
            HelperSpec = storage:get_helper_spec(StorageData),
            {Configuration, Credentials} = helper_spec:describe(HelperSpec),
            LumaConfig = storage:get_luma_config(StorageData),

            {ok, #storage_description{
                id = StorageId,
                name = storage:fetch_name_of_local_storage(StorageId),
                type = helper_spec:get_name(HelperSpec),
                timeout = helper_spec:get_timeout(HelperSpec),
                readonly = storage:is_local_storage_readonly(StorageId),
                imported = storage:is_imported(StorageId),
                luma = #luma_spec{
                    feed = luma_config:get_feed(LumaConfig),
                    url = luma_config:get_url(LumaConfig),
                    api_key = luma_config:get_api_key(LumaConfig)
                },
                qos_parameters = storage:fetch_qos_parameters_of_local_storage(StorageId),
                credentials = Credentials,
                configuration = Configuration
            }};

        {error, not_found} ->
            ?ERROR_NOT_FOUND
    end.
