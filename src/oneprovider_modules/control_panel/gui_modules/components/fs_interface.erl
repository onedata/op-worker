%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains functions for operating on file system. This is for page modules to
%% be able to use the file system API without knowledge of underlying structures.
%% @end
%% ===================================================================
-module(fs_interface).

-include("oneprovider_modules/dao/dao_vfs.hrl").

%% API
-export([file_parts_mock/1]).

file_parts_mock(_FilePath) ->
    random:seed(now()),
    Size = random:uniform(123213) + 500,

    {Size, [
        {<<"provider1">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])},
        {<<"prdfsgsdfovider2">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])},
        {<<"provr3">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])},
        {<<"prosdfvider4">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])}
    ]}.