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

-export([get/3, get_public_handle_url/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get(
    oct_background:entity_selector(), oct_background:entity_selector(), od_handle:id()
) -> od_handle:doc().
get(ProviderSelector, UserSelector, HandleId) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    SessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    {ok, Handle} =  ?rpc(Node, handle_logic:get(SessId, HandleId)),
    Handle.


-spec get_public_handle_url(oct_background:entity_selector(), oct_background:entity_selector(), od_handle:id()) ->
    od_handle:public_handle().
get_public_handle_url(ProviderSelector, UserSelector, HandleId) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    SessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    {ok, #document{value = #od_handle{public_handle = PublicHandle}}} = ?rpc(
        Node, handle_logic:get_public_data(SessId, HandleId)
    ),
    PublicHandle.