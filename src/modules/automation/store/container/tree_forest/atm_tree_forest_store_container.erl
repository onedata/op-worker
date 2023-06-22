%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `tree forest`
%%% atm_store type. Uses `atm_list_store_container` for storing list of roots.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_store_container).
-author("Michal Stanisz").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_store_container callbacks
-export([
    create/1,
    copy/1,
    get_config/1,

    get_iterated_item_data_spec/1,
    acquire_iterator/1,

    browse_content/2,
    update_content/2,

    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

-type initial_content() :: [automation:item()] | undefined.

-type content_browse_req() :: #atm_store_content_browse_req{
    options :: atm_tree_forest_store_content_browse_options:record()
}.
-type content_update_req() :: #atm_store_content_update_req{
    options :: atm_tree_forest_store_content_update_options:record()
}.

-record(atm_tree_forest_store_container, {
    config :: atm_tree_forest_store_config:record(),
    roots_list :: atm_list_store_container:record()
}).
-type record() :: #atm_tree_forest_store_container{}.

-export_type([
    initial_content/0, content_browse_req/0, content_update_req/0,
    record/0
]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_store_container:creation_args()) -> record() | no_return().
create(CreationArgs = #atm_store_container_creation_args{store_config = AtmStoreConfig}) ->
    #atm_tree_forest_store_container{
        config = AtmStoreConfig,
        roots_list = atm_list_store_container:create(CreationArgs#atm_store_container_creation_args{
            store_config = #atm_list_store_config{
                item_data_spec = AtmStoreConfig#atm_tree_forest_store_config.item_data_spec
            }
        })
    }.


-spec copy(record()) -> no_return().
copy(_) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec get_config(record()) -> atm_tree_forest_store_config:record().
get_config(#atm_tree_forest_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(#atm_tree_forest_store_container{
    config = #atm_tree_forest_store_config{item_data_spec = ItemDataSpec}
}) ->
    ItemDataSpec.


-spec acquire_iterator(record()) -> atm_tree_forest_store_container_iterator:record().
acquire_iterator(#atm_tree_forest_store_container{
    config = #atm_tree_forest_store_config{item_data_spec = ItemDataSpec},
    roots_list = RootsList
}) ->
    RootsIterator = atm_list_store_container:acquire_iterator(RootsList),
    atm_tree_forest_store_container_iterator:build(ItemDataSpec, RootsIterator).


-spec browse_content(record(), content_browse_req()) ->
    atm_tree_forest_store_content_browse_result:record() | no_return().
browse_content(Record, ContentBrowseReq = #atm_store_content_browse_req{
    options = #atm_tree_forest_store_content_browse_options{listing_opts = ListingOpts}
}) ->
    Result = atm_list_store_container:browse_content(
        Record#atm_tree_forest_store_container.roots_list,
        ContentBrowseReq#atm_store_content_browse_req{
            options = #atm_list_store_content_browse_options{listing_opts = ListingOpts}
        }
    ),
    #atm_tree_forest_store_content_browse_result{
        tree_roots = Result#atm_list_store_content_browse_result.items,
        is_last = Result#atm_list_store_content_browse_result.is_last
    }.


-spec update_content(record(), content_update_req()) -> record() | no_return().
update_content(Record, UpdateReq = #atm_store_content_update_req{
    options = #atm_tree_forest_store_content_update_options{function = Function}
}) ->
    RootsList = Record#atm_tree_forest_store_container.roots_list,
    RootsListContentUpdateReq = UpdateReq#atm_store_content_update_req{
        options = #atm_list_store_content_update_options{function = Function}
    },
    Record#atm_tree_forest_store_container{
        roots_list = atm_list_store_container:update_content(
            RootsList, RootsListContentUpdateReq
        )
    }.


-spec delete(record()) -> ok.
delete(#atm_tree_forest_store_container{roots_list = RootsList}) ->
    atm_list_store_container:delete(RootsList).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_tree_forest_store_container{
    config = AtmStoreConfig,
    roots_list = ListContainer
}, NestedRecordEncoder) ->
    #{
        <<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_tree_forest_store_config),
        <<"rootsList">> => NestedRecordEncoder(ListContainer, atm_list_store_container)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    #{<<"config">> := AtmStoreConfigJson, <<"rootsList">> := EncodedListContainer},
    NestedRecordDecoder
) ->
    #atm_tree_forest_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_tree_forest_store_config),
        roots_list = NestedRecordDecoder(EncodedListContainer, atm_list_store_container)
    }.
