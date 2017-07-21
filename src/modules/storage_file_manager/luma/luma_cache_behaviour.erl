%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Temporary cache behaviour for luma models.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_cache_behaviour).
-author("Jakub Kudzia").

%%--------------------------------------------------------------------
%% @doc
%% Returns timestamp of last save.
%% @end
%%--------------------------------------------------------------------
-callback last_timestamp(ModelRecord :: luma_cache:model()) ->
    Timestamp :: luma_cache:timestamp().

%%--------------------------------------------------------------------
%% @doc
%% Returns saved value from model record.
%% @end
%%--------------------------------------------------------------------
-callback get_value(ModelRecord :: luma_cache:model()) ->
    Value :: luma_cache:value().

%%--------------------------------------------------------------------
%% @doc
%% Returns new model record.
%% @end
%%--------------------------------------------------------------------
-callback new(Value :: luma_cache:value(), Timestamp :: luma_cache:timestamp()) ->
    ModelRecord :: luma_cache:model().