%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour for modules implementing special directories.
%%% @end
%%%-------------------------------------------------------------------
-module(special_dir_behaviour).
-author("Michal Stanisz").

-optional_callbacks([get_file_meta/1, get_times/2]).

%%%===================================================================
%%% Callbacks
%%%===================================================================

-callback is_special(uuid | guid, file_meta:uuid() | file_id:file_guid()) -> boolean().

-callback allowed_operations() -> [middleware_worker:operation() | fslogic_worker:operation()].

-callback is_filesystem_root_dir() -> boolean().

-callback can_be_shared() -> boolean().

-callback is_affected_by_protection_flags() -> boolean().

-callback is_included_in_harvesting() -> boolean().

-callback is_included_in_dir_stats() -> boolean().

-callback is_included_in_events() -> boolean().

-callback is_logically_detached() -> boolean().

-callback exists(file_meta:uuid()) -> boolean().

%%%===================================================================
%%% Optional callbacks
%%%===================================================================

-callback get_file_meta(file_meta:uuid()) -> file_meta:doc().

-callback get_times(file_meta:uuid(), [times_api:times_type()]) -> times:record().
