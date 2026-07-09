import os
import happybase
import json
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from jinja2 import Template

app = FastAPI()

HBASE_HOST = os.environ.get('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.environ.get('HBASE_PORT', '9090'))
HBASE_TABLE = os.environ.get('HBASE_TABLE_NAME', 'pokemon')

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HBase Pokémon Explorer</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        body {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #090d16 0%, #111827 100%);
            min-height: 100vh;
            color: #f8fafc;
        }
        .glass-card {
            background: rgba(17, 24, 39, 0.7);
            backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.05);
            box-shadow: 0 10px 40px 0 rgba(0, 0, 0, 0.45);
        }
        .pokemon-card {
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .pokemon-card:hover {
            transform: translateY(-8px);
            border-color: rgba(99, 102, 241, 0.3);
            box-shadow: 0 15px 35px rgba(99, 102, 241, 0.12);
        }
        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #090d16;
        }
        ::-webkit-scrollbar-thumb {
            background: #1f2937;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #374151;
        }
    </style>
</head>
<body class="p-6 md:p-12">
    <div class="max-w-7xl mx-auto">
        <!-- Header -->
        <header class="mb-14 text-center md:text-left flex flex-col md:flex-row md:items-center md:justify-between gap-6">
            <div>
                <h1 class="text-4xl font-extrabold tracking-tight bg-gradient-to-r from-indigo-400 via-purple-400 to-pink-400 bg-clip-text text-transparent">
                    HBase Pokémon Explorer
                </h1>
                <p class="text-slate-400 mt-2 font-medium">
                    Real-time viewer for HBase Table: <code class="text-indigo-300 bg-indigo-950/40 px-2.5 py-0.5 rounded border border-indigo-900/40">{{ table_name }}</code>
                </p>
            </div>
            
            <div class="flex items-center gap-3 justify-center">
                <span class="inline-flex items-center px-3.5 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                    <span class="w-2 h-2 mr-2 bg-emerald-400 rounded-full animate-ping"></span>
                    HBase Connected
                </span>
                <span class="text-slate-700">|</span>
                <span class="text-sm text-slate-400 font-bold bg-slate-900/60 px-3 py-1 rounded-lg border border-slate-800">Total: {{ total_count }} Pokémon</span>
            </div>
        </header>

        <!-- Search & Filters -->
        <div class="glass-card rounded-2xl p-6 mb-16 flex flex-col md:flex-row gap-4">
            <div class="flex-1 relative">
                <input type="text" id="search" placeholder="Search by name, surname, ID, or description..." 
                       class="w-full bg-slate-950/80 border border-slate-800/80 rounded-xl px-4 py-3.5 pl-11 focus:outline-none focus:border-indigo-500 text-slate-200 placeholder-slate-500 transition-colors"
                       onkeyup="filterPokemon()">
                <svg class="w-5 h-5 absolute left-4 top-4 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
                </svg>
            </div>
            
            <div class="w-full md:w-64">
                <select id="type-filter" onchange="filterPokemon()" 
                        class="w-full bg-slate-950/80 border border-slate-800/80 rounded-xl px-4 py-3.5 focus:outline-none focus:border-indigo-500 text-slate-300 transition-colors cursor-pointer">
                    <option value="">All Types</option>
                    {% for type in all_types %}
                    <option value="{{ type }}">{{ type }}</option>
                    {% endfor %}
                </select>
            </div>
        </div>

        <!-- Pokemon Grid -->
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-y-16 gap-x-6" id="pokemon-grid">
            {% for p in pokemon_list %}
            <div class="pokemon-card glass-card rounded-2xl p-6 pt-16 flex flex-col justify-between relative mt-6" 
                 data-name="{{ p.name_english.lower() }} {{ p.surname.lower() }} {{ p.french_surname.lower() }} {{ p.id }} {{ p.description.lower() }}"
                 data-types="{{ p.types | join(',') }}">
                
                <!-- Floating Avatar -->
                <div class="absolute -top-14 left-1/2 transform -translate-x-1/2">
                    <div class="w-28 h-28 rounded-full bg-slate-900 border border-slate-800/60 flex items-center justify-center p-2.5 shadow-2xl overflow-hidden group">
                        {% if p.image_url %}
                        <img src="{{ p.image_url }}" alt="{{ p.name_english }}" 
                             class="w-24 h-24 object-contain transition-transform duration-300 group-hover:scale-110"
                             loading="lazy">
                        {% else %}
                        <span class="text-xs text-slate-600 font-semibold tracking-wider">No Image</span>
                        {% endif %}
                    </div>
                </div>

                <div>
                    <!-- ID & Types -->
                    <div class="flex justify-between items-start mb-4">
                        <span class="text-xs font-extrabold text-slate-500 tracking-wider">#{{ "%03d" | format(p.id | int) }}</span>
                        <div class="flex gap-1">
                            {% for type in p.types %}
                            <span class="px-2 py-0.5 rounded-md text-[10px] font-bold uppercase tracking-wider border
                                         {% if type.lower() == 'grass' %} bg-emerald-500/10 text-emerald-400 border-emerald-500/20
                                         {% elif type.lower() == 'poison' %} bg-fuchsia-500/10 text-fuchsia-400 border-fuchsia-500/20
                                         {% elif type.lower() == 'fire' %} bg-orange-500/10 text-orange-400 border-orange-500/20
                                         {% elif type.lower() == 'water' %} bg-sky-500/10 text-sky-400 border-sky-500/20
                                         {% elif type.lower() == 'bug' %} bg-lime-500/10 text-lime-400 border-lime-500/20
                                         {% elif type.lower() == 'flying' %} bg-teal-500/10 text-teal-400 border-teal-500/20
                                         {% elif type.lower() == 'normal' %} bg-slate-400/10 text-slate-300 border-slate-400/20
                                         {% elif type.lower() == 'electric' %} bg-amber-500/10 text-amber-400 border-amber-500/20
                                         {% elif type.lower() == 'ground' %} bg-amber-700/10 text-amber-500 border-amber-700/20
                                         {% elif type.lower() == 'fairy' %} bg-pink-500/10 text-pink-400 border-pink-500/20
                                         {% elif type.lower() == 'fighting' %} bg-rose-500/10 text-rose-400 border-rose-500/20
                                         {% elif type.lower() == 'psychic' %} bg-purple-500/10 text-purple-400 border-purple-500/20
                                         {% elif type.lower() == 'rock' %} bg-yellow-600/10 text-yellow-500 border-yellow-600/20
                                         {% elif type.lower() == 'steel' %} bg-slate-300/10 text-slate-200 border-slate-300/20
                                         {% elif type.lower() == 'ice' %} bg-cyan-400/10 text-cyan-300 border-cyan-400/20
                                         {% elif type.lower() == 'ghost' %} bg-indigo-400/10 text-indigo-300 border-indigo-400/20
                                         {% elif type.lower() == 'dragon' %} bg-violet-600/10 text-violet-400 border-violet-600/20
                                         {% else %} bg-slate-500/10 text-slate-400 border-slate-500/20 {% endif %}">
                                {{ type }}
                            </span>
                            {% endfor %}
                        </div>
                    </div>

                    <!-- Names -->
                    <div class="mb-4">
                        <h2 class="text-2xl font-bold text-slate-100 tracking-tight">{{ p.name_english }}</h2>
                        <div class="flex gap-2 text-xs text-slate-400 mt-1 font-semibold">
                            <span>{{ p.name_japanese }}</span>
                            <span>•</span>
                            <span>{{ p.species }}</span>
                        </div>
                        {% if p.surname or p.french_surname %}
                        <div class="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-2">
                            <div class="rounded-lg bg-slate-950/70 border border-slate-800 px-3 py-2">
                                <div class="text-[10px] font-bold uppercase tracking-wider text-slate-500">Surname</div>
                                <div class="text-sm font-semibold text-indigo-200">{{ p.surname or "Pending" }}</div>
                            </div>
                            <div class="rounded-lg bg-slate-950/70 border border-slate-800 px-3 py-2">
                                <div class="text-[10px] font-bold uppercase tracking-wider text-slate-500">French</div>
                                <div class="text-sm font-semibold text-pink-200">{{ p.french_surname or "Pending" }}</div>
                            </div>
                        </div>
                        {% endif %}
                    </div>

                    <!-- Description -->
                    <p class="text-sm text-slate-400 leading-relaxed mb-6 italic line-clamp-3">
                        "{{ p.description }}"
                    </p>
                </div>

                <!-- Stats -->
                <div class="space-y-2.5 border-t border-slate-800/80 pt-4">
                    <div class="flex items-center justify-between text-xs font-semibold text-slate-400">
                        <span class="w-10">HP</span>
                        <span class="w-8 text-right text-slate-300">{{ p.hp }}</span>
                        <div class="flex-1 bg-slate-950 rounded-full h-1.5 ml-3 relative overflow-hidden">
                            <div class="bg-gradient-to-r from-emerald-500 to-green-400 h-1.5 rounded-full" style="width: {{ [p.hp / 2.5, 100] | min }}%"></div>
                        </div>
                    </div>
                    <div class="flex items-center justify-between text-xs font-semibold text-slate-400">
                        <span class="w-10">ATK</span>
                        <span class="w-8 text-right text-slate-300">{{ p.attack }}</span>
                        <div class="flex-1 bg-slate-950 rounded-full h-1.5 ml-3 relative overflow-hidden">
                            <div class="bg-gradient-to-r from-rose-500 to-red-400 h-1.5 rounded-full" style="width: {{ [p.attack / 2.5, 100] | min }}%"></div>
                        </div>
                    </div>
                    <div class="flex items-center justify-between text-xs font-semibold text-slate-400">
                        <span class="w-10">DEF</span>
                        <span class="w-8 text-right text-slate-300">{{ p.defense }}</span>
                        <div class="flex-1 bg-slate-950 rounded-full h-1.5 ml-3 relative overflow-hidden">
                            <div class="bg-gradient-to-r from-sky-500 to-indigo-400 h-1.5 rounded-full" style="width: {{ [p.defense / 2.5, 100] | min }}%"></div>
                        </div>
                    </div>
                    <div class="flex items-center justify-between text-xs font-semibold text-slate-400">
                        <span class="w-10">SPD</span>
                        <span class="w-8 text-right text-slate-300">{{ p.speed }}</span>
                        <div class="flex-1 bg-slate-950 rounded-full h-1.5 ml-3 relative overflow-hidden">
                            <div class="bg-gradient-to-r from-amber-500 to-orange-400 h-1.5 rounded-full" style="width: {{ [p.speed / 2.5, 100] | min }}%"></div>
                        </div>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>

    <script>
        function filterPokemon() {
            const searchValue = document.getElementById('search').value.toLowerCase();
            const typeValue = document.getElementById('type-filter').value.toLowerCase();
            const cards = document.querySelectorAll('.pokemon-card');

            cards.forEach(card => {
                const searchMatch = card.getAttribute('data-name').includes(searchValue);
                const types = card.getAttribute('data-types').toLowerCase().split(',');
                const typeMatch = !typeValue || types.includes(typeValue);

                if (searchMatch && typeMatch) {
                    card.style.display = 'flex';
                } else {
                    card.style.display = 'none';
                }
            });
        }
    </script>
</body>
</html>
"""


def fetch_pokemon_from_hbase():
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
        connection.open()

        tables = connection.tables()
        table_bytes = HBASE_TABLE.encode('utf-8')
        if table_bytes not in tables:
            print(f"HBase table '{HBASE_TABLE}' does not exist.")
            connection.close()
            return []

        table = connection.table(HBASE_TABLE)
        pokemon_list = []

        for key, data in table.scan():
            pokemon_id = key.decode('utf-8')
            
            # Retrieve available image keys (hires -> thumbnail -> sprite)
            image_url = ""
            for img_key in [b'info:image_hires', b'info:image_thumbnail', b'info:image_sprite']:
                if img_key in data:
                    val = data[img_key].decode('utf-8')
                    if val:
                        image_url = val
                        break

            pokemon = {
                'id': pokemon_id,
                'name_english': data.get(b'name:english', b'').decode('utf-8') or f"Pokémon {pokemon_id}",
                'name_japanese': data.get(b'name:japanese', b'').decode('utf-8'),
                'surname': data.get(b'name:surname', b'').decode('utf-8'),
                'french_surname': data.get(b'name:frenchSurname', b'').decode('utf-8'),
                'species': data.get(b'info:species', b'').decode('utf-8'),
                'description': data.get(b'info:description', b'').decode('utf-8'),
                'hp': int(data.get(b'base:HP', b'0').decode('utf-8')),
                'attack': int(data.get(b'base:Attack', b'0').decode('utf-8')),
                'defense': int(data.get(b'base:Defense', b'0').decode('utf-8')),
                'speed': int(data.get(b'base:Speed', b'0').decode('utf-8')),
                'image_url': image_url
            }

            types = []
            idx = 0
            while True:
                type_key = f"type:{idx}".encode('utf-8')
                if type_key in data:
                    types.append(data[type_key].decode('utf-8'))
                    idx += 1
                else:
                    break
            pokemon['types'] = types
            pokemon_list.append(pokemon)

        connection.close()
        pokemon_list.sort(key=lambda x: int(x['id']) if x['id'].isdigit() else 9999)
        return pokemon_list
    except Exception as e:
        print(f"Error fetching from HBase: {e}")
        return []


@app.get("/", response_class=HTMLResponse)
def index():
    pokemon_list = fetch_pokemon_from_hbase()

    # Extract unique types
    types_set = set()
    for p in pokemon_list:
        for t in p['types']:
            types_set.add(t)
    all_types = sorted(list(types_set))

    # Render Template
    t = Template(HTML_TEMPLATE)
    html_content = t.render(
        pokemon_list=pokemon_list,
        total_count=len(pokemon_list),
        table_name=HBASE_TABLE,
        all_types=all_types
    )
    return HTMLResponse(content=html_content)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8081)
