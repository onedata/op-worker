%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This behaviour defines API for `recursive_listing` callback modules.
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_listing_behaviour).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").

-type object() :: recursive_listing:object().
-type object_id() :: recursive_listing:object_id().
-type name() :: recursive_listing:name().
-type path() :: recursive_listing:path().
-type limit() :: recursive_listing:limit().
-type object_listing_opts() :: recursive_listing:object_listing_opts().

%%%===================================================================
%%% API
%%%===================================================================

-callback is_traversable_object(object()) -> {boolean(), object()}.

-callback get_object_id(object()) -> object_id().

-callback get_object_name(object(), user_ctx:ctx() | undefined) -> {name(), object()}.

-callback get_object_path(object()) -> {path(), object()}.

-callback get_parent_id(object(), user_ctx:ctx()) -> object_id().

-callback build_listing_opts(name(), limit(), boolean(), object_id()) -> object_listing_opts().

-callback check_access(object(), user_ctx:ctx()) -> ok | {error, ?EACCES}.

-callback list_children_with_access_check(object(), object_listing_opts(), user_ctx:ctx()) -> 
    {ok, [object()], object_listing_opts(), object()} | {error, ?EACCES}.

-callback is_listing_finished(object_listing_opts()) -> boolean().

