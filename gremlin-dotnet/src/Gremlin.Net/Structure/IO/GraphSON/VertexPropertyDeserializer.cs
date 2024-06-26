﻿#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System.Linq;
using System.Text.Json;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class VertexPropertyDeserializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JsonElement graphsonObject, GraphSONReader reader)
        {
            var id = reader.ToObject(graphsonObject.GetProperty("id"));
            string label = graphsonObject.GetProperty("label").GetString()!;
            var value = reader.ToObject(graphsonObject.GetProperty("value"));
            var vertex = graphsonObject.TryGetProperty("vertex", out var vertexProperty)
                ? new Vertex(reader.ToObject(vertexProperty))
                : null;

            Property[]? properties = null;
            if (graphsonObject.TryGetProperty("properties", out var propertiesObject)
                && propertiesObject.ValueKind == JsonValueKind.Object)
            {
                properties = propertiesObject.EnumerateObject()
                    .Select(p => new Property(p.Name, reader.ToObject(p.Value))).ToArray();
            }
            return new VertexProperty(id, label, value, vertex, properties);
        }
    }
}