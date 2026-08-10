// ── Kortlek ──────────────────────────────────────────────────────

const SUITS = ['spader', 'hjärter', 'ruter', 'klöver'];
const VALUES = ['2','3','4','5','6','7','8','9','10','Kn','D','K','E'];
const VALUE_RANK = { '2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9,'10':10,'Kn':11,'D':12,'K':13,'E':14 };

// Skapa en komplett kortlek (52 kort)
function createDeck() {
  const deck = [];
  for (const suit of SUITS) {
    for (const value of VALUES) {
      deck.push({ suit, value, rank: VALUE_RANK[value] });
    }
  }
  return deck;
}

// Blanda kortleken (Fisher-Yates)
function shuffleDeck(deck) {
  const d = [...deck];
  for (let i = d.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [d[i], d[j]] = [d[j], d[i]];
  }
  return d;
}

// Kupera kortleken (dela på slumpmässig position)
function cutDeck(deck) {
  const cutPoint = Math.floor(deck.length * 0.25 + Math.random() * deck.length * 0.5);
  return [...deck.slice(cutPoint), ...deck.slice(0, cutPoint)];
}

// Dela ut kort till spelare
function dealCards(deck, players, numCards) {
  const hands = {};
  players.forEach(p => hands[p.id] = []);
  
  let cardIndex = 0;
  for (let i = 0; i < numCards; i++) {
    for (const player of players) {
      if (cardIndex < deck.length) {
        hands[player.id].push(deck[cardIndex++]);
      }
    }
  }
  return hands;
}

// Beräkna omgångssekvensen baserat på antal spelare och max kort
function buildRoundSequence(numPlayers, maxCards) {
  // Anpassa max kort baserat på antal spelare
  let effectiveMax = maxCards;
  if (numPlayers >= 7) effectiveMax = Math.min(maxCards, 7);
  else if (numPlayers === 6) effectiveMax = Math.min(maxCards, 8);
  else if (numPlayers === 5) effectiveMax = Math.min(maxCards, 10);

  const sequence = [];
  
  // Nedåt: från max till 2
  for (let i = effectiveMax; i >= 2; i--) {
    sequence.push(i);
  }
  
  // Ettor: antal = antal spelare
  for (let i = 0; i < numPlayers; i++) {
    sequence.push(1);
  }
  
  // Uppåt: från 2 till max
  for (let i = 2; i <= effectiveMax; i++) {
    sequence.push(i);
  }
  
  return sequence;
}

// Avgör vem som vinner ett stick
// Returnerar index i cards-arrayen som vann
function determineStickWinner(cards, leadSuit) {
  let winnerIdx = 0;
  let highestRank = -1;
  
  for (let i = 0; i < cards.length; i++) {
    const card = cards[i];
    if (card.suit === leadSuit && card.rank > highestRank) {
      highestRank = card.rank;
      winnerIdx = i;
    }
  }
  return winnerIdx;
}

module.exports = { createDeck, shuffleDeck, cutDeck, dealCards, buildRoundSequence, determineStickWinner, SUITS, VALUES, VALUE_RANK };
