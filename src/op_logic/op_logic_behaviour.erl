%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour should be implemented by modules that implement op logic
%%% operations. Every op logic plugin serves as a middleware between
%%% API and op internals (e.g. lfm) in the context of specific
%%% entity type (op_xxx records).
%%% TODO VFS-5620
%%% @end
%%%-------------------------------------------------------------------
-module(op_logic_behaviour).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-callback operation_supported(
    op_logic:operation(), op_logic:aspect(), op_logic:scope()
) ->
    boolean().


%%--------------------------------------------------------------------
%% @doc
%% Returns data spec for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be sanitized.
%% @end
%%--------------------------------------------------------------------
-callback data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity and its revision from datastore based on EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-callback fetch_entity(entity_logic:entity_id()) ->
    {ok, entity_logic:versioned_entity()} | entity_logic:error().


%%--------------------------------------------------------------------
%% @doc
%% Determines if given resource (aspect of entity) exists, based on
%% op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-callback exists(op_logic:req(), op_logic:entity()) -> boolean().


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-callback authorize(op_logic:req(), op_logic:entity()) -> boolean().


%%--------------------------------------------------------------------
%% @doc
%% Determines if given request can be further processed
%% (e.g. checks whether space is supported locally).
%% Should throw custom error if not (e.g. ?ERROR_SPACE_NOT_SUPPORTED).
%% @end
%%--------------------------------------------------------------------
-callback validate(op_logic:req(), op_logic:entity()) -> ok | no_return().


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
