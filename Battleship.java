import java.util.Scanner;
public class Battleship {
	public static void main(String[] args) {
		System.out.println("Welcome to Battleship!");
		System.out.println();

		char[][] player1Ships = getPositions(1);
		for (int i = 0; i < 100; i++){
			System.out.println();
		}
		char[][] player2Ships = getPositions(2);

		for (int i = 0; i < 100; i++){
			System.out.println();
		}

		char[][] player1Moves = new char[][]{
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'}
		};

		char[][] player2Moves = new char[][]{
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'}
		};

		// execute moves until one players ships are all attacked
		int[] remainingShips = {5, 5};

		int player = 1;

		while (remainingShips[0] > 0 && remainingShips[1] > 0){
			if (player == 1) {
				remainingShips = play(player, player1Moves, player2Ships, remainingShips);
				player = 2;
			}
			else{
				remainingShips = play(player, player2Moves, player1Ships, remainingShips);
				player = 1;
			}
		}
		System.out.printf("PLAYER %d WINS! YOU SUNK ALL OF YOUR OPPONENT’S SHIPS!", (player-1));
		
	}


	// use this method to play a move in the game
	private static int[] play(int playerID, char[][] attacksBoard, char[][] opponentsBoard, int[] remainingShips){
		// get a valid move from the player
		Scanner input = new Scanner(System.in);
		int opponentID = playerID == 1 ? 2 : 1 ; 
		int x = -1;
		int y = -1;
		System.out.println("Player " + playerID + ", enter hit row/column:");
		boolean validMove = false;
		while (validMove == false){
			input.reset();

			if (input.hasNextInt()){
				x = input.nextInt();
				if (x >= opponentsBoard.length || x < 0){
					System.out.println("Invalid coordinates. Choose different coordinates.");
					continue;
				}
				if (input.hasNextInt()){
					y = input.nextInt(); 
					if (y >= opponentsBoard.length || y < 0){
						System.out.println("Invalid coordinates. Choose different coordinates.");
						continue;
					}
				}
				if (attacksBoard[x][y] != '-'){
					System.out.println("You already fired on this spot. Choose different coordinates.");
					continue;
					}
				else {validMove = true;}
		}
		}
		// execute the move
		if (opponentsBoard[x][y] == '@'){
			attacksBoard[x][y] = 'X';
			System.out.println("PLAYER " + playerID + " HIT PLAYER " + opponentID + "'s SHIP!");
			remainingShips[(opponentID-1)] -= 1;
		}
		else{
			attacksBoard[x][y] = 'O';
			System.out.println("PLAYER " + playerID + " MISSED!");
		}
		printBattleShip(attacksBoard);

		// update the scores
		return remainingShips;

	}

	//use this method to set up a players board
	private static char[][] getPositions(int playerID){
		Scanner input = new Scanner(System.in);

		char[][] board = new char[][]{
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'},
			{'-', '-', '-', '-', '-'}
		};

		System.out.println("PLAYER " + playerID + ", ENTER YOUR SHIPS' COORDINATES");
		// get coordinates for 5 ships 
		int position = 1;
		do {
			System.out.println("Enter ship " + position + " location:");
			if (input.hasNextInt()){
                int x = input.nextInt();
				if (x >= board.length || x < 0){
					System.out.println("Invalid coordinates. Choose different coordinates.");
					input.nextLine();
					continue;
				}
                if (input.hasNextInt()){
                    int y = input.nextInt(); 
					if (y >= board.length || y < 0){
						System.out.println("Invalid coordinates. Choose different coordinates.");
						input.nextLine();
						continue;
					}
					if (board[x][y] == '@'){
						System.out.println("You already have a ship there. Choose different coordinates.");
						input.nextLine();
						continue;
					}
					board[x][y] = '@';
					position++;
				}
			}
			else{
				System.out.println("Invalid coordinates. Choose different coordinates.");
				input.nextLine();
			}
		}
		while (position < 6);
		printBattleShip(board);
		return board;
	}

	// Use this method to print game boards to the console.
	private static void printBattleShip(char[][] player) {
		System.out.print("  ");
		for (int row = -1; row < 5; row++) {
			if (row > -1) {
				System.out.print(row + " ");
			}
			for (int column = 0; column < 5; column++) {
				if (row == -1) {
					System.out.print(column + " ");
				} else {
					System.out.print(player[row][column] + " ");
				}
			}
			System.out.println("");
		}
	}
}