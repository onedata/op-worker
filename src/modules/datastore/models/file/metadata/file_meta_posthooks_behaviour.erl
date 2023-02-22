%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour of a module, that is using file meta posthooks.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_posthooks_behaviour).
-author("Michal Stanisz").

%%%===================================================================
%%% Callbacks
%%%===================================================================

-callback encode_file_meta_posthook_args(file_meta_posthooks:function_name(), [term()]) ->
    file_meta_posthooks:encoded_args().

-callback decode_file_meta_posthook_args(file_meta_posthooks:function_name(), file_meta_posthooks:encoded_args()) ->
    [term()].
