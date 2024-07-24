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

-callback is_operation_allowed(atom()) -> boolean().

-callback exists(file_meta:uuid()) -> boolean().


%%%===================================================================
%%% Optional callbacks
%%%===================================================================

-callback get_file_meta(file_meta:uuid()) -> file_meta:doc().

-callback get_times(file_meta:uuid(), [times_api:times_type()]) -> times:record().