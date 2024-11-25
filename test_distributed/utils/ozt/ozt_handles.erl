%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions related to handles operations in Onezone to be used
%%% in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(ozt_handles).
-author("Katarzyna Such").

-include("onenv_test_utils.hrl").

-export([create/6]).
-export([example_metadata_variant/2, expected_metadata_after_publication/2]).

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


-spec create(
    oct_background:entity_selector(), oct_background:entity_selector(), od_share:id(),
    od_handle_service:id(), od_handle:metadata_prefix(), od_handle:metadata()
) -> od_handle:id().
create(ProviderSelector, UserSelector, ShareId, HServiceId, MetadataPrefix, MetadataString) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    SessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    {ok, HandleId} = ?rpc(Node, handle_logic:create(
        SessId, HServiceId, <<"Share">>, ShareId, MetadataPrefix, MetadataString
    )),
    HandleId.


-spec example_metadata_variant(od_handle:metadata_prefix(), integer()) -> binary().
example_metadata_variant(<<"oai_dc">>, 1) ->
    ?EXAMPLE_METADATA1;
example_metadata_variant(<<"oai_dc">>, 2) ->
    ?EXAMPLE_METADATA2.


-spec expected_metadata_after_publication(binary(), binary()) -> binary().
expected_metadata_after_publication(InputMetadata, PublicHandle) ->
    case InputMetadata of
        ?EXAMPLE_METADATA1 ->
            <<
                "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
                "<metadata>\n",
                "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>",
                "    <dc:contributor>John Doe</dc:contributor>",
                "</metadata>"
            >>;
        ?EXAMPLE_METADATA2 ->
            <<
                "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
                "<metadata>\n",
                "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>",
                "    <dc:contributor>Jane Doe</dc:contributor>",
                "    <dc:description>Lorem ipsum</dc:description>",
                "</metadata>"
            >>
    end.

