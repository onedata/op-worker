%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions related to handles operations in Oneprovider to be used
%%% in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_handles).
-author("Katarzyna Such").

-include("modules/datastore/datastore_models.hrl").

-export([get_metadata/3, get_public_handle_url/3]).
-export([create/4, create/6]).


-define(DEFAULT_METADATA_SCHEMA, <<"oai_dc">>).
-define(DEFAULT_METADATA, <<
    "<?xml version=\"1.0\" encoding=\"utf-8\"?>
    <metadata>",
    "    <dc:contributor>John Doe</dc:contributor>",
    "</metadata>"
>>).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_metadata(oct_background:node_selector(), oct_background:entity_selector(), od_handle:id()) ->
    od_handle:metadata().
get_metadata(NodeSelector, UserSelector, HandleId) ->
    {ok, #document{value = #od_handle{metadata = Metadata}}} = get(NodeSelector, UserSelector, HandleId),
    Metadata.


-spec get_public_handle_url(oct_background:node_selector(), oct_background:entity_selector(), od_handle:id()) ->
    od_handle:public_handle().
get_public_handle_url(NodeSelector, UserSelector, HandleId) ->
    {ok, #document{value = #od_handle{public_handle = PublicHandle}}} = get(NodeSelector, UserSelector, HandleId),
    PublicHandle.


-spec create(
    oct_background:node_selector(), oct_background:entity_selector(),
    od_share:id(), od_handle_service:id()
) -> od_handle:id().
create(NodeSelector, UserSelector, ShareId, HServiceId) ->
    create(NodeSelector, UserSelector, ShareId, HServiceId, ?DEFAULT_METADATA_SCHEMA, ?DEFAULT_METADATA).


-spec create(
    oct_background:node_selector(), oct_background:entity_selector(), od_share:id(),
    od_handle_service:id(), od_handle:metadata_schema(), od_handle:metadata()
) -> od_handle:id().
create(NodeSelector, UserSelector, ShareId, HServiceId, MetadataSchema, MetadataString) ->
    SessId = oct_background:get_user_session_id(UserSelector, NodeSelector),
    {ok, HandleId} = opw_test_rpc:call(NodeSelector, handle_logic, create, [
        SessId, HServiceId, <<"Share">>, ShareId, MetadataSchema, MetadataString
    ]),
    HandleId.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get(oct_background:node_selector(), oct_background:entity_selector(), od_handle:id()) ->
    {ok, od_handle:doc()} | errors:error().
get(NodeSelector, UserSelector, HandleId) ->
    SessId = oct_background:get_user_session_id(UserSelector, NodeSelector),
    opw_test_rpc:call(NodeSelector, handle_logic, get_public_data, [SessId, HandleId]).

