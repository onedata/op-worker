%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour should be implemented by modules that implement op logic
%%% operations. Every op logic plugin serves as a middleware between
%%% API and datastore in the context of specific entity type (op_xxx records).
%%% @end
%%%-------------------------------------------------------------------
-module(op_logic_behaviour).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity from datastore based on its EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-callback fetch_entity(op_logic:entity_id()) ->
    {ok, op_logic:entity()} | op_logic:error().
-optional_callbacks([fetch_entity/1]).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-callback operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-callback create(op_logic:req()) -> op_logic:create_result().


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-callback get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().


%%--------------------------------------------------------------------
%% @doc
%% Updates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-callback update(op_logic:req()) -> op_logic:update_result().


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-callback delete(op_logic:req()) -> op_logic:delete_result().


%%--------------------------------------------------------------------
%% @doc
%% Determines if given resource (aspect of entity) exists, based on
%% op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-callback exists(op_logic:req(), op_logic:entity()) -> boolean().
-optional_callbacks([exists/2]).


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-callback authorize(op_logic:req(), op_logic:entity()) -> boolean().


%%--------------------------------------------------------------------
%% @doc
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-callback data_signature(op_logic:req()) -> op_validator:data_signature().
