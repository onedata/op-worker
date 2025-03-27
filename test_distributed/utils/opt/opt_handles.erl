%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
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

-include("onenv_test_utils.hrl").

-export([get/3, get_metadata/3, get_public_handle_url/3]).
-export([create/4, create/6]).
-export([example_metadata_variant/2, expected_metadata_after_publication/2]).


-define(DEFAULT_METADATA_PREFIX, <<"oai_dc">>).

-define(EXAMPLE_METADATA1, <<
    "<?xml version=\"1.0\" encoding=\"utf-8\"?>
    <metadata>",
    "    <dc:contributor>John Doe</dc:contributor>",
    "</metadata>"
>>).

-define(EXAMPLE_METADATA2, <<
    "<?xml version=\"1.0\" encoding=\"utf-8\"?>
    <metadata>",
    "    <dc:contributor>Jane Doe</dc:contributor>",
    "    <dc:description>Lorem ipsum</dc:description>",
    "</metadata>"
>>).


%%%===================================================================
%%% API
%%%===================================================================


-spec get(oct_background:node_selector(), oct_background:entity_selector(), od_handle:id()) ->
    {ok, od_handle:doc()} | errors:error().
get(NodeSelector, UserSelector, HandleId) ->
    SessId = oct_background:get_user_session_id(UserSelector, NodeSelector),
    test_rpc:call(op_worker, NodeSelector, handle_logic, get_public_data, [SessId, HandleId]).


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
    create(NodeSelector, UserSelector, ShareId, HServiceId, ?DEFAULT_METADATA_PREFIX, ?EXAMPLE_METADATA1).


-spec create(
    oct_background:node_selector(), oct_background:entity_selector(), od_share:id(),
    od_handle_service:id(), od_handle:metadata_prefix(), od_handle:metadata()
) -> od_handle:id().
create(NodeSelector, UserSelector, ShareId, HServiceId, MetadataPrefix, MetadataString) ->
    Node = oct_background:get_random_provider_node(NodeSelector),
    SessId = oct_background:get_user_session_id(UserSelector, NodeSelector),

    {ok, HandleId} = ?rpc(Node, handle_logic:create(
        SessId, HServiceId, <<"Share">>, ShareId, MetadataPrefix, MetadataString
    )),
    HandleId.


-spec example_metadata_variant(od_handle:metadata_prefix(), integer()) -> binary().
example_metadata_variant(?DEFAULT_METADATA_PREFIX, 1) ->
    ?EXAMPLE_METADATA1;
example_metadata_variant(?DEFAULT_METADATA_PREFIX, 2) ->
    ?EXAMPLE_METADATA2.


-spec expected_metadata_after_publication(binary(), binary()) -> binary().
expected_metadata_after_publication(?EXAMPLE_METADATA1, PublicHandle) ->
    <<
        "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
        "<metadata>\n",
        "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>",
        "    <dc:contributor>John Doe</dc:contributor>",
        "</metadata>"
    >>;
expected_metadata_after_publication(?EXAMPLE_METADATA2, PublicHandle) ->
    <<
        "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
        "<metadata>\n",
        "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>",
        "    <dc:contributor>Jane Doe</dc:contributor>",
        "    <dc:description>Lorem ipsum</dc:description>",
        "</metadata>"
    >>.
