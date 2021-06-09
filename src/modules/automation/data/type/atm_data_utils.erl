%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains utility functions for `atm_data` related modules.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_utils).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").

-export([is_error_ignored/1]).
-export([get_callback_module/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec is_error_ignored(errors:errno()) -> boolean().
is_error_ignored(?EACCES) -> true;
is_error_ignored(?EPERM) -> true;
is_error_ignored(?ENOENT) -> true;
is_error_ignored(_) -> false.


-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_dataset_type) -> atm_dataset_value;
get_callback_module(atm_file_type) -> atm_file_value;
get_callback_module(atm_integer_type) -> atm_integer_value;
get_callback_module(atm_string_type) -> atm_string_value;
get_callback_module(atm_object_type) -> atm_object_value.
