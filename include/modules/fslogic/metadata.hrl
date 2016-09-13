%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common definitions and macros for metadata
%%% @end
%%%-------------------------------------------------------------------
-author("Lukasz Opiola").

-define(METADATA_INTERNAL_PREFIXES, [<<"onedata_">>, <<"cdmi_">>]).

-define(IS_INTERNAL_PREFIX(__Binary),
    begin
        lists:foldl(
            fun(__InternalPrefix, __Acc) ->
                __StartsWith = case __Binary of
                    <<__InternalPrefix/binary, _/binary>> ->
                        true;
                    _ ->
                        __Acc
                end,
                __Acc orelse __StartsWith
            end, false, ?METADATA_INTERNAL_PREFIXES)
    end).