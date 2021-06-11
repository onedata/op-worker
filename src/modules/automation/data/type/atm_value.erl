%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Atm value modules are Oneprovider specific implementation of atm_data_type's. 
%%% Each must implement `atm_data_validator` and `atm_data_compressor` behaviours. 
%%% Each value is saved in store in its compressed form and is expanded upon listing.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_value).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").

-export([get_callback_module/1]).
-export([is_error_ignored/1]).

-type compressed() :: binary().
-type expanded() :: atm_api:item().

-export_type([compressed/0, expanded/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_dataset_type) -> atm_dataset_value;
get_callback_module(atm_file_type) -> atm_file_value;
get_callback_module(atm_integer_type) -> atm_integer_value;
get_callback_module(atm_string_type) -> atm_string_value;
get_callback_module(atm_object_type) -> atm_object_value.


-spec is_error_ignored(errors:reason()) -> boolean().
is_error_ignored(?EACCES) -> true;
is_error_ignored(?EPERM) -> true;
is_error_ignored(?ENOENT) -> true;
is_error_ignored(not_found) -> true;
is_error_ignored(_) -> false.
