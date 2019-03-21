%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This behaviour describes server side logic of model synchronization
%%% between ember and the server. The API is analogous to ember adapter API.
%%% The handler module is resolved by calling op_gui_routes:data_backend/1.
%%% The module returned from that function should implement this behaviour.
%%% Returning op_gui_error:error_result() from any of the callbacks will cause the
%%% message to be displayed on client side, so the messages must be readable
%%% for users.
%%% @end
%%%-------------------------------------------------------------------
-module(data_backend_behaviour).
-author("Lukasz Opiola").


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Called on initialization of WebSocket connection. This is where async
%% processes can be started using op_gui_async module. They can asynchronously
%% push messages about model changes to the client.
%% @end
%%--------------------------------------------------------------------
-callback init() -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Called on finalization of WebSocket connection. This is where all cleanup
%% should be performed.
%% @end
%%--------------------------------------------------------------------
-callback terminate() -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Called when ember store tries to find one or more records. Should return
%% a list of objects (proplists that will be encoded to JSON) corresponding
%% to the list of requested Ids.
%% ResourceType is the name of the model used in ember.
%% @end
%%--------------------------------------------------------------------
-callback find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().


%%--------------------------------------------------------------------
%% @doc
%% Called when ember store tries to find all records of given type.
%% Should return a list of objects (proplists that will be encoded to JSON).
%% ResourceType is the name of the model used in ember.
%% @end
%%--------------------------------------------------------------------
-callback find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().


%%--------------------------------------------------------------------
%% @doc
%% Called when ember store tries to find all records matching given properties.
%% For example, all files of type directory: Data=[{<<"type">>, <<"dir">>}].
%% Should return a list of objects (proplists that will be encoded to JSON).
%% ResourceType is the name of the model used in ember.
%% @end
%%--------------------------------------------------------------------
-callback query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().


%%--------------------------------------------------------------------
%% @doc
%% Called when ember store tries to find ONE record matching given properties.
%% For example, a space with given id: Data=[{<<"id">>, <<"space_id">>}].
%% Should return an object (proplist that will be encoded to JSON).
%% ResourceType is the name of the model used in ember.
%% @end
%%--------------------------------------------------------------------
-callback query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().


%%--------------------------------------------------------------------
%% @doc
%% Called when ember store tries to create a record.
%% Should return a created object (proplist that will be encoded to JSON).
%% The Id of new object must be generated and included in the response.
%% ResourceType is the name of the model used in ember.
%% @end
%%--------------------------------------------------------------------
-callback create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().


%%--------------------------------------------------------------------
%% @doc
%% Called when ember store tries to update a record.
%% Should return updated object (proplist that will be encoded to JSON).
%% ResourceType is the name of the model used in ember.
%% @end
%%--------------------------------------------------------------------
-callback update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | op_gui_error:error_result().


%%--------------------------------------------------------------------
%% @doc
%% Called when ember store tries to delete a record.
%% ResourceType is the name of the model used in ember.
%% @end
%%--------------------------------------------------------------------
-callback delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
