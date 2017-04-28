defmodule ETL do
  @doc """
  Transform an index into an inverted index.

  ## Examples

  iex> ETL.transform(%{"a" => ["ABILITY", "AARDVARK"], "b" => ["BALLAST", "BEAUTY"]})
  %{"ability" => "a", "aardvark" => "a", "ballast" => "b", "beauty" =>"b"}
  """
  @spec transform(map) :: map
  def transform(input) do
    _transform(input)
  end

  defp _transform(input) do
    _transform_iterator(Map.to_list(input))
  end

  # defp _transform_iterator([], acc), do: acc
  defp _transform_iterator(kvps) do
    kvps
      |> Enum.reduce(%{}, fn(kvp, acc) -> Map.merge(acc, _invert_kvp(kvp)) end)
  end

  defp _invert_kvp({key, values}) do
    values
      |> Enum.map(fn(value) -> %{String.downcase(value) => key} end)
      |> Enum.reduce(%{}, fn(map, acc) -> Map.merge(acc, map) end)
  end

end
